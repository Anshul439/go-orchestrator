package worker

import (
	"context"
	"github.com/anshul439/go-orchestrator/internal/db"
	"github.com/anshul439/go-orchestrator/internal/queue"
	"github.com/jackc/pgx/v5/pgxpool"
	"log/slog"
	"math"
	"math/rand"
	"sync"
	"time"
)

type Pool struct {
	workerCount int
	queue       queue.Queue
	db          *pgxpool.Pool
	wg          sync.WaitGroup
	logger      *slog.Logger
}

func NewPool(
	workerCount int,
	q queue.Queue,
	conn *pgxpool.Pool,
	logger *slog.Logger,
) *Pool {

	return &Pool{
		workerCount: workerCount,
		queue:       q,
		db:          conn,
		logger:      logger,
	}
}

func (p *Pool) Start(ctx context.Context) {

	for i := 1; i <= p.workerCount; i++ {

		p.wg.Add(1)

		go p.worker(ctx, i)
	}
}

func (p *Pool) worker(ctx context.Context, id int) {

	defer p.wg.Done()

	for {
		if ctx.Err() != nil {
			p.logger.Info(
				"worker shutting down",
				slog.Int("worker_id", id),
			)

			return
		}

		job, err := p.queue.Consume(ctx)

		if err != nil {
			if ctx.Err() != nil {
				p.logger.Info(
					"worker shutting down",
					slog.Int("worker_id", id),
				)
				return
			}

			p.logger.Error(
				"queue consume failed",
				slog.Int("worker_id", id),
				slog.String("error", err.Error()),
			)
			continue
		}

		err = db.UpdateJobState(
			p.db,
			job.ID,
			"running",
			job.RetryCount,
		)

		if err != nil {
			p.logger.Error(
				"database operation failed",
				slog.String("error", err.Error()),
			)
		}

		p.logger.Info(
			"processing job",
			slog.Int("worker_id", id),
			slog.Int("job_id", job.ID),
		)

		if !waitForProcessing(ctx, 2*time.Second) {
			p.logger.Info(
				"worker shutting down during job",
				slog.Int("worker_id", id),
				slog.Int("job_id", job.ID),
			)
			return
		}

		// simulate random failure
		failed := rand.Intn(2) == 0

		if failed {

			p.logger.Warn(
				"job failed",
				slog.Int("worker_id", id),
				slog.Int("job_id", job.ID),
				slog.Int("retry_count", job.RetryCount),
			)

			// retry if retries remaining
			if job.RetryCount < job.MaxRetries {

				job.RetryCount++

				err := db.UpdateJobState(
					p.db,
					job.ID,
					"retrying",
					job.RetryCount,
				)

				if err != nil {
					p.logger.Error(
						"database operation failed",
						slog.String("error", err.Error()),
					)
				}

				// exponential backoff: 2^retryCount seconds
				delay := time.Duration(
					math.Pow(2, float64(job.RetryCount)),
				) * time.Second

				p.logger.Info(
					"retrying job",
					slog.Int("job_id", job.ID),
					slog.Duration("delay", delay),
					slog.Int("retry_count", job.RetryCount),
					slog.Int("max_retries", job.MaxRetries),
				)

				if retryErr := p.queue.Retry(
					ctx,
					job,
					delay,
				); retryErr != nil {
					p.logger.Error(
						"queue retry scheduling failed",
						slog.Int("job_id", job.ID),
						slog.String("error", retryErr.Error()),
					)
				}

			} else {

				err := db.UpdateJobState(
					p.db,
					job.ID,
					"failed",
					job.RetryCount,
				)

				if err != nil {
					p.logger.Error(
						"database operation failed",
						slog.String("error", err.Error()),
					)
				}

				p.logger.Error(
					"job permanently failed",
					slog.Int("job_id", job.ID),
					slog.Int("retry_count", job.RetryCount),
				)

				if failErr := p.queue.Fail(
					ctx,
					job,
				); failErr != nil {
					p.logger.Error(
						"queue fail cleanup failed",
						slog.Int("job_id", job.ID),
						slog.String("error", failErr.Error()),
					)
				}
			}

			continue
		}

		err = db.UpdateJobState(
			p.db,
			job.ID,
			"completed",
			job.RetryCount,
		)

		if err != nil {
			p.logger.Error(
				"database operation failed",
				slog.String("error", err.Error()),
			)
		}

		p.logger.Info(
			"job completed",
			slog.Int("worker_id", id),
			slog.Int("job_id", job.ID),
		)

		if ackErr := p.queue.Ack(
			ctx,
			job,
		); ackErr != nil {
			p.logger.Error(
				"queue ack failed",
				slog.Int("job_id", job.ID),
				slog.String("error", ackErr.Error()),
			)
		}
	}
}

func (p *Pool) Wait() {
	p.wg.Wait()
}

// waitForProcessing sleeps for delay, returning false if ctx is cancelled.
func waitForProcessing(
	ctx context.Context,
	delay time.Duration,
) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false

	case <-timer.C:
		return true
	}
}
