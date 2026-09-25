package workerclient

import (
	"context"
	"fmt"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/domain"
	chronosv1 "github.com/ghrushneshr25/chronos-dev/proto/chronos/v1"
	"google.golang.org/grpc"
)

// Client pushes jobs to a worker over gRPC.
type Client struct {
	inner chronosv1.WorkerServiceClient
}

func New(conn grpc.ClientConnInterface) *Client {
	return &Client{inner: chronosv1.NewWorkerServiceClient(conn)}
}

func (c *Client) ExecuteJob(ctx context.Context, job domain.QueuedJob, executionID string) error {
	timeout := job.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	resp, err := c.inner.ExecuteJob(ctx, &chronosv1.ExecuteJobRequest{
		JobId:       job.JobID,
		ExecutionId: executionID,
		Name:        job.Name,
		Type:        string(job.Type),
		Payload:     job.Payload,
		Attempt:     int32(job.Attempt),
		TimeoutMs:   timeout.Milliseconds(),
		TraceId:     job.TraceID,
	})
	if err != nil {
		return err
	}
	if resp == nil || !resp.Accepted {
		msg := "rejected"
		if resp != nil {
			msg = resp.Message
		}
		return fmt.Errorf("worker rejected job: %s", msg)
	}
	return nil
}

func (c *Client) CancelJob(ctx context.Context, jobID, executionID string) error {
	_, err := c.inner.CancelJob(ctx, &chronosv1.CancelJobRequest{
		JobId:       jobID,
		ExecutionId: executionID,
	})
	return err
}
