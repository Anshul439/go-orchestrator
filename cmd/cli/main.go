package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	pb "github.com/anshul439/go-orchestrator/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func usage() {
	fmt.Println("usage:")
	fmt.Println("  go run ./cmd/cli submit [max-retries]")
	fmt.Println("  go run ./cmd/cli status <job-id>")
}

func grpcTarget() string {
	target := os.Getenv("GRPC_ADDR")
	if target == "" {
		target = "localhost:50051"
	}

	// Server listen addresses often look like ":50051".
	// For the client, treat that as localhost on the same port.
	if strings.HasPrefix(target, ":") {
		return "localhost" + target
	}

	return target
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	conn, err := grpc.NewClient(
		grpcTarget(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		fmt.Println("error creating gRPC client:", err)
		os.Exit(1)
	}

	defer conn.Close()

	client := pb.NewOrchestratorServiceClient(conn)

	switch os.Args[1] {
	case "submit":
		retries := int32(3) // default if not provided
		if len(os.Args) > 2 {
			n, err := strconv.Atoi(os.Args[2])
			if err != nil {
				fmt.Println("error: max retries must be a number")
				os.Exit(1)
			}
			retries = int32(n)
		}

		resp, err := client.SubmitJob(context.Background(), &pb.SubmitJobRequest{MaxRetries: retries})
		if err != nil {
			fmt.Println("error:", err)
			os.Exit(1)
		}
		fmt.Println("job submitted, id:", resp.JobId)

	case "status":
		if len(os.Args) < 3 {
			fmt.Println("error: missing job id")
			usage()
			os.Exit(1)
		}

		id, err := strconv.Atoi(os.Args[2])
		if err != nil {
			fmt.Println("error: job id must be a number")
			os.Exit(1)
		}

		resp, err := client.GetJob(context.Background(), &pb.GetJobRequest{JobId: int32(id)})
		if err != nil {
			fmt.Println("error:", err)
			os.Exit(1)
		}
		fmt.Printf("job %d: status=%s retries=%d/%d\n",
			resp.JobId, resp.Status, resp.RetryCount, resp.MaxRetries)

	default:
		fmt.Printf("error: unknown command %q\n", os.Args[1])
		usage()
		os.Exit(1)
	}
}
