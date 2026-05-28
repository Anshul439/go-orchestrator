# go-orchestrator

Background job queue with retries, persistence, and graceful shutdown — built in Go with Redis and Postgres.

## Current Features

- Worker pool for concurrent job execution
- Redis-backed queue abstraction
- Postgres-backed job persistence
- Retry handling with exponential backoff
- Recovery for in-flight Redis jobs on restart
- Structured logging
- Graceful shutdown

## Requirements

- Go
- PostgreSQL
- Redis

## Configuration

Copy `.env.example` to `.env` and adjust values if needed.

## Setup

```bash
createdb -U postgres orchestrator
psql -U postgres -d orchestrator -f schema.sql
```

## Running

```bash
go run .
```

On startup, the app seeds `DEMO_JOB_COUNT` demo jobs, pushes them to Redis, and workers begin processing them.