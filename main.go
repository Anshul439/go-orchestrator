package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/anshul439/go-orchestrator/internal/config"
	"github.com/anshul439/go-orchestrator/internal/db"
	"github.com/anshul439/go-orchestrator/internal/logger"
	"github.com/anshul439/go-orchestrator/internal/queue"
	"github.com/anshul439/go-orchestrator/internal/worker"
	gredis "github.com/redis/go-redis/v9"
)

func main() {
	log := logger.NewLogger()

	cfg := config.LoadConfig()

	ctx, cancel := context.WithCancel(context.Background())

	poolConn, err := db.NewPostgresPool(cfg.DBUrl)

	if err != nil {
		log.Error(
			"failed to connect to postgres",
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}

	redisClient := gredis.NewClient(&gredis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	err = redisClient.Ping(ctx).Err()

	if err != nil {
		log.Error(
			"failed to connect to redis",
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}

	log.Info("connected to postgres")
	log.Info("connected to redis")

	defer poolConn.Close()
	defer redisClient.Close()

	q := queue.NewRedisQueue(
		redisClient,
		cfg.RedisQueueName,
		time.Second,
	)

	if err := db.ResetRunningJobs(poolConn); err != nil {
		log.Error(
			"failed to reset running jobs",
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}

	if err := q.Recover(ctx); err != nil {
		log.Error(
			"failed to recover redis queue",
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}

	q.Start(ctx)

	pool := worker.NewPool(
		cfg.WorkerCount,
		q,
		poolConn,
		log,
	)

	pool.Start(ctx)

	for i := 0; i < cfg.DemoJobCount; i++ {
		jobID, err := db.InsertJob(poolConn, 3)

		if err != nil {
			log.Error(
				"failed to insert demo job",
				slog.String("error", err.Error()),
			)
			os.Exit(1)
		}

		job := queue.Job{
			ID:         jobID,
			MaxRetries: 3,
		}

		if err := q.Enqueue(ctx, job); err != nil {
			log.Error(
				"failed to enqueue demo job",
				slog.Int("job_id", job.ID),
				slog.String("error", err.Error()),
			)
			os.Exit(1)
		}
	}

	signalChan := make(chan os.Signal, 1)

	signal.Notify(
		signalChan,
		os.Interrupt,
		syscall.SIGTERM,
	)

	<-signalChan

	cancel()

	pool.Wait()
}
