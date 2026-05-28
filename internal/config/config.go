package config

import (
	"errors"
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	DBUrl          string
	WorkerCount    int
	RedisAddr      string
	RedisPassword  string
	RedisDB        int
	RedisQueueName string
	DemoJobCount   int
}

func LoadConfig() *Config {

	err := godotenv.Load()

	if err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Fatal("Error loading .env file")
	}

	workerCount, err := strconv.Atoi(
		os.Getenv("WORKER_COUNT"),
	)

	if err != nil {
		log.Fatal(err)
	}

	redisDB := 0

	redisDBValue := os.Getenv("REDIS_DB")

	if redisDBValue != "" {
		redisDB, err = strconv.Atoi(redisDBValue)

		if err != nil {
			log.Fatal(err)
		}
	}

	demoJobCount := 10

	demoJobCountValue := os.Getenv("DEMO_JOB_COUNT")

	if demoJobCountValue != "" {
		demoJobCount, err = strconv.Atoi(demoJobCountValue)

		if err != nil {
			log.Fatal(err)
		}
	}

	return &Config{
		DBUrl:          os.Getenv("DB_URL"),
		WorkerCount:    workerCount,
		RedisAddr:      getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:  os.Getenv("REDIS_PASSWORD"),
		RedisDB:        redisDB,
		RedisQueueName: getEnv("REDIS_QUEUE_NAME", "jobs"),
		DemoJobCount:   demoJobCount,
	}
}

func getEnv(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	return value
}
