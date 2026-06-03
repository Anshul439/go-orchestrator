package server

import (
	"context"

	"github.com/anshul439/go-orchestrator/internal/db"
	"github.com/anshul439/go-orchestrator/internal/queue"
	pb "github.com/anshul439/go-orchestrator/proto"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Server implements the generated OrchestratorServiceServer interface.
type Server struct {
	pb.UnimplementedOrchestratorServiceServer // required embed — handles unimplemented RPCs gracefully
	db                                        *pgxpool.Pool
	queue                                     queue.Queue
}

func New(db *pgxpool.Pool, q queue.Queue) *Server {
	return &Server{db: db, queue: q}
}

func (s *Server) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	jobID, err := db.InsertJob(s.db, int(req.MaxRetries))
	if err != nil {
		return nil, err
	}

	job := queue.Job{ID: jobID, MaxRetries: int(req.MaxRetries)}
	if err := s.queue.Enqueue(ctx, job); err != nil {
		return nil, err
	}

	return &pb.SubmitJobResponse{JobId: int32(jobID)}, nil
}

func (s *Server) GetJob(ctx context.Context, req *pb.GetJobRequest) (*pb.GetJobResponse, error) {
	job, err := db.GetJob(s.db, int(req.JobId))
	if err != nil {
		return nil, err
	}

	return &pb.GetJobResponse{
		JobId:      int32(job.ID),
		Status:     job.Status,
		RetryCount: int32(job.RetryCount),
		MaxRetries: int32(job.MaxRetries),
	}, nil
}
