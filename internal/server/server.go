package server

import (
	"context"

	"github.com/anshul439/go-orchestrator/internal/db"
	"github.com/anshul439/go-orchestrator/internal/queue"
	pb "github.com/anshul439/go-orchestrator/proto"
	"github.com/jackc/pgx/v5/pgxpool"
	"math"
	"time"
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
	jobID, err := db.InsertJob(s.db, int(req.MaxRetries), req.Type, req.Payload)
	if err != nil {
		return nil, err
	}

	job := queue.Job{ID: jobID, MaxRetries: int(req.MaxRetries), Type: req.Type, Payload: req.Payload}
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
		Type:       job.Type,
		Payload:    job.Payload,
	}, nil

}

func (s *Server) Work(stream pb.OrchestratorService_WorkServer) error {
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}

		switch p := msg.Payload.(type) {
		case *pb.WorkerMessage_Ready:
			job, err := s.queue.Consume(stream.Context())
			if err != nil {
				return err
			}

			if err := db.UpdateJobState(s.db, job.ID, "running", job.RetryCount); err != nil {
				return err
			}

			stream.Send(&pb.ServerMessage{
				Payload: &pb.ServerMessage_Task{
					Task: &pb.TaskAssignment{
						JobId:      int32(job.ID),
						Type:       job.Type,
						Payload:    job.Payload,
						RetryCount: int32(job.RetryCount),
						MaxRetries: int32(job.MaxRetries),
					},
				},
			})

		case *pb.WorkerMessage_Result:
			s.handleResult(stream.Context(), p.Result)
		}
	}
}

func (s *Server) handleResult(ctx context.Context, result *pb.TaskResult) {
	jobID := int(result.JobId)

	if result.Success {
		row, err := db.GetJob(s.db, jobID)
		if err != nil {
			return
		}
		s.queue.Ack(ctx, queue.Job{ID: jobID})
		db.UpdateJobState(s.db, jobID, "completed", row.RetryCount)
		return
	}

	row, err := db.GetJob(s.db, jobID)
	if err != nil {
		return
	}

	job := queue.Job{
		ID:         row.ID,
		Type:       row.Type,
		Payload:    row.Payload,
		RetryCount: row.RetryCount,
		MaxRetries: row.MaxRetries,
	}

	if job.RetryCount < job.MaxRetries {
		job.RetryCount++
		delay := time.Duration(math.Pow(2, float64(job.RetryCount))) * time.Second
		db.UpdateJobState(s.db, job.ID, "retrying", job.RetryCount)
		s.queue.Retry(ctx, job, delay)
	} else {
		db.UpdateJobState(s.db, job.ID, "failed", job.RetryCount)
		s.queue.Fail(ctx, job)
	}
}

func (s *Server) ListJobs(ctx context.Context, req *pb.ListJobsRequest) (*pb.ListJobsResponse, error) {
	jobs, err := db.ListJobs(s.db, req.Status)
	if err != nil {
		return nil, err
	}

	var resp []*pb.GetJobResponse
	for _, j := range jobs {
		resp = append(resp, &pb.GetJobResponse{
			JobId:      int32(j.ID),
			Status:     j.Status,
			RetryCount: int32(j.RetryCount),
			MaxRetries: int32(j.MaxRetries),
			Type:       j.Type,
			Payload:    j.Payload,
		})
	}

	return &pb.ListJobsResponse{Jobs: resp}, nil
}
