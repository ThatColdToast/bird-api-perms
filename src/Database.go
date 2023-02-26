package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/eko/gocache/lib/v4/cache"
	"github.com/eko/gocache/lib/v4/store"
	redis_store "github.com/eko/gocache/store/redis/v4"
	redis "github.com/go-redis/redis/v8"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type Database struct {
	uri        string
	username   string
	password   string
	redisCache *cache.Cache[string]
	// redisStore *redis_store.RedisStore
}

func makeDatabase() Database {
	return Database{
		uri:      os.Getenv("NEO4J_URI"),
		username: os.Getenv("NEO4J_USERNAME"),
		password: os.Getenv("NEO4J_PASSWORD"),

		redisCache: cache.New[string](redis_store.NewRedis(redis.NewClient(
			&redis.Options{
				Addr: "172.17.0.3:6379",
			},
		))),
	}
}

func (x Database) getUserPerms(userId string) ([]string, error) { //context.WithDeadline(context.Background(), time.Now().Add(30*time.Second))
	cachedValue, err := x.redisCache.Get(context.Background(), userId)

	switch err {
	case nil:
		// log.Printf("Cached Value '%s': '%s'\n", userId, cachedValue)
		return strings.Split(cachedValue, " "), nil
	case redis.Nil:
		log.Printf("Cache Miss '%s'\n", userId)
	default:
		log.Printf("Cache Miss/Error '%s': %v\n", userId, err.Error())
	}

	value, err := x.getUserPermsFromDatabase(context.Background(), userId)
	if err != nil {
		return []string{}, err
	}

	cache_err := x.redisCache.Set(
		context.Background(),
		userId,
		value,
		store.WithExpiration(15*time.Second),
	)

	if cache_err != nil {
		// panic(cache_err)
		return []string{}, cache_err
	}

	return strings.Split(value, " "), nil
}

func (x Database) getUserPermsFromDatabase(ctx context.Context, userId string) (string, error) {

	driver, err := neo4j.NewDriverWithContext(x.uri, neo4j.BasicAuth(x.username, x.password, ""))
	if err != nil {
		return "", err
	}
	defer driver.Close(ctx)

	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	greeting, err := session.ExecuteRead(ctx, func(transaction neo4j.ManagedTransaction) (any, error) {
		result, err := transaction.Run(ctx,
			// "MATCH (n:User {id:$userId})-[:HAS_TRAIT]->(t:Trait) RETURN COLLECT(t.permissions)",
			// "MATCH (n:User {id:$userId})-[:HAS_TRAIT]->(t:Trait) UNWIND(t.permissions) AS x RETURN x ORDER BY t.weight DESC",
			`
MATCH (:User {id:$userId})-[:HAS_TRAIT]->(t:Trait) UNWIND(t.permissions) AS x
WITH x, t.weight AS weight
ORDER BY weight DESC
RETURN COLLECT(x)
			`,
			map[string]any{"userId": userId},
		)

		if err != nil {
			return nil, err
		}

		if result.Next(ctx) {
			record := result.Record()
			value := fmt.Sprintf("%s", record.Values[0])
			perms := value[1 : len(value)-1]
			// log.Printf("Perms: %s", perms)
			return perms, nil
			// return result.Record().Values[0], nil
		}

		return nil, result.Err()
	})
	if err != nil {
		return "", err
	}

	return greeting.(string), nil
}
