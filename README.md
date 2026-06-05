# go-orchestrator

A background job processing system built in Go. Jobs are submitted over gRPC, queued in Redis, persisted in Postgres, and executed by a worker pool with retry and recovery.

## Features

- gRPC API for job submission and status queries
- Job type and payload — route different kinds of work to appropriate handlers
- CLI client (`cmd/cli`) for submitting and inspecting jobs
- Worker pool for concurrent job execution
- Redis-backed queue with reliable delivery (`BRPOPLPUSH` pattern)
- Postgres-backed job persistence
- Exponential backoff retry with configurable max retries
- Crash recovery — in-flight jobs are re-queued on restart
- Delayed job scheduling via Redis sorted sets
- Structured logging (`log/slog`)
- Graceful shutdown

## Architecture

```
cmd/cli  ──gRPC──►  cmd/server
                        ├── internal/server   (gRPC handlers)
                        ├── internal/queue    (Redis queue)
                        ├── internal/worker   (worker pool)
                        └── internal/db       (Postgres)
```

## Requirements

- Go 1.21+
- PostgreSQL
- Redis
- [Task](https://taskfile.dev/)
- [`golang-migrate`](https://github.com/golang-migrate/migrate)
- `protoc` + Go plugins (only needed to regenerate proto)

## Setup

```bash
# Create database
createdb -U postgres orchestrator

# Copy env config
cp .env.example .env

# Apply migrations
task migrate:up
```

## Configuration

| Variable          | Default              | Description                        |
|-------------------|----------------------|------------------------------------|
| `DB_URL`          | —                    | Postgres connection string         |
| `WORKER_COUNT`    | —                    | Number of concurrent workers       |
| `REDIS_ADDR`      | `localhost:6379`     | Redis address                      |
| `REDIS_PASSWORD`  | —                    | Redis password (leave blank if none) |
| `REDIS_DB`        | `0`                  | Redis DB index                     |
| `REDIS_QUEUE_NAME`| `jobs`               | Redis key prefix for queues        |
| `GRPC_ADDR`       | `:50051`             | gRPC server listen address         |

## Migrations

Create a new migration:

```bash
task migrate:create NAME=<name>
```

Apply all pending migrations:

```bash
task migrate:up
```

Roll back the most recent migration:

```bash
task migrate:down
```

Check the current migration version:

```bash
task migrate:version
```

## Running

```bash
# Start the orchestrator server
task run
```

## CLI Usage

```bash
# Start the server first
task run

# Submit a job with type and payload
go run ./cmd/cli submit --type=send_email --payload='{"to":"x@y.com"}'

# Submit with custom max retries
go run ./cmd/cli submit --type=resize_image --payload='{"url":"s3://img.jpg"}' --retries=5

# Submit with defaults (type=generic, payload={}, retries=3)
go run ./cmd/cli submit

# Check job status
go run ./cmd/cli status <job-id>
```

Example output:
```
job submitted, id: 42 (type=send_email)
job 42 (send_email): status=completed retries=1/3
```

The CLI reads `GRPC_ADDR` too. If it is set to a listen-style value like `:50051`,
the CLI treats it as `localhost:50051`.

## Regenerating Proto

If you modify `proto/orchestrator.proto`, regenerate Go code with:

```bash
task proto
```

Requires `protoc-gen-go` and `protoc-gen-go-grpc` in your `$PATH`:
```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```
