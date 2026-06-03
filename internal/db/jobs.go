package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

func InsertJob(
	conn *pgxpool.Pool,
	maxRetries int,
) (int, error) {

	var jobID int

	query := `
		INSERT INTO jobs (
			status,
			retry_count,
			max_retries
		)
		VALUES ($1, $2, $3)
		RETURNING id
	`

	err := conn.QueryRow(
		context.Background(),
		query,
		"pending",
		0,
		maxRetries,
	).Scan(&jobID)

	if err != nil {
		return 0, err
	}

	return jobID, nil
}

func UpdateJobState(
	conn *pgxpool.Pool,
	jobID int,
	state string,
	retryCount int,
) error {

	query := `
		UPDATE jobs
		SET
			status = $1,
			retry_count = $2,
			updated_at = NOW()
		WHERE id = $3
	`

	_, err := conn.Exec(
		context.Background(),
		query,
		state,
		retryCount,
		jobID,
	)

	return err
}

// ResetRunningJobs resets 'running' jobs back to 'pending' after a crash.
func ResetRunningJobs(
	conn *pgxpool.Pool,
) error {

	query := `
		UPDATE jobs
		SET
			status = 'pending',
			updated_at = NOW()
		WHERE status = 'running'
	`

	_, err := conn.Exec(
		context.Background(),
		query,
	)

	return err
}

// JobRow is the DB representation of a job.
// Distinct from queue.Job, which is the lightweight in-memory struct used by the queue and workers.
type JobRow struct {
    ID         int
    Status     string
    RetryCount int
	MaxRetries int
}

func GetJob(conn *pgxpool.Pool, jobID int) (JobRow, error) {
    var row JobRow

 query := `SELECT id, status, retry_count, max_retries FROM jobs WHERE id = $1`
    err := conn.QueryRow(context.Background(), query, jobID).
        Scan(&row.ID, &row.Status, &row.RetryCount, &row.MaxRetries)

    return row, err
}
