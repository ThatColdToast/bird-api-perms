package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func readMessages() {
	log.Printf("Connecting\n")

	username := os.Getenv("PERMISSION_RABBITMQ_USERNAME")
	password := os.Getenv("PERMISSION_RABBITMQ_PASSWORD")
	host := os.Getenv("RABBITMQ_SVC_SERVICE_HOST")
	port := os.Getenv("RABBITMQ_SVC_SERVICE_PORT")
	queue_name := os.Getenv("PERMISSION_RABBITMQ_QUEUE_NAME")

	// log.Printf("Us:'%s' Pa:'%s' Ho:'%s' Po:'%s' QN:'%s'\n", username, password, host, port, queue_name)
	// log.Printf("amqp://%s:%s@%s:%s/\nQN:%s\n", username, password, host, port, queue_name)

	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/", username, password, host, port))
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	log.Printf("Creating Channel\n")
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	log.Printf("Creating Queue\n")
	queue, err := ch.QueueDeclare(
		queue_name, // name
		false,      // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// Setting QOS so we don't get too many responses
	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	log.Printf("Consuming\n")
	msgs, err := ch.Consume(
		queue.Name, // queue
		"",         // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	failOnError(err, "Failed to register a consumer")

	database := makeDatabase()

	for msg := range msgs {
		handleMessage(&msg, ch, &database)
	}
}

func parsePerms(perms []string, query []string) bool {
	for i := 0; i < len(perms); i++ {
		// log.Printf("Owned: %s", perms[i])
		perm := strings.Split(perms[i], ".")

		var allowed bool = false
		if string(perm[0][0]) != "!" {
			allowed = true
		} else {
			perm[0] = perm[0][1:]
		}

		for j := 0; j < len(perm); j++ {
			// log.Printf("Check: %s : %s", perm[j], query[j])
			if perm[j] != query[j] {
				if perm[j] == "*" {
					return allowed
				}
				break
			}

			if j == len(perm)-1 {
				return allowed
			}
		}
	}
	return false
}

func handleMessage(msg *amqp.Delivery, ch *amqp.Channel, database *Database) {
	request := strings.Split(string(msg.Body), "|")
	// log.Printf("Request: %s", request[1])

	// perms = ["bird.post.create", "bird.post.read.any"]
	perms, err := database.getUserPerms(request[0])
	if err != nil {
		log.Println("Error:", err)
	}

	var allowed string = "false"
	if parsePerms(perms, strings.Split(request[1], ".")) {
		allowed = "true"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ch.PublishWithContext(ctx,
		"",          // exchange
		msg.ReplyTo, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: msg.CorrelationId,
			Body:          []byte(allowed),
		})
	failOnError(err, "Failed to publish a message")

	// The key is not to ack until
	// I've queried the database successfully
	// And successfully send a return on the reply queue
	msg.Ack(true)
}

func main() {
	// readMessages()

	var wg sync.WaitGroup

	threads := 6
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func() {
			log.Printf("[%d]\n", i)
			readMessages()

			// Never reached
			wg.Done()
		}()
	}

	wg.Wait()
}
