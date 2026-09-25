package worker

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/config"
	"github.com/ghrushneshr25/chronos-dev/internal/domain"
	"github.com/ghrushneshr25/chronos-dev/internal/execution"
	chronosv1 "github.com/ghrushneshr25/chronos-dev/proto/chronos/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Worker struct {
	chronosv1.UnimplementedWorkerServiceServer

	cfg      config.WorkerConfig
	token    string
	executor *execution.Executor
	log      *slog.Logger

	active  atomic.Int32
	mu      sync.Mutex
	cancels map[string]context.CancelFunc

	gs *grpc.Server
}

func New(cfg config.WorkerConfig, token string, exec *execution.Executor, log *slog.Logger) *Worker {
	if log == nil {
		log = slog.Default()
	}
	return &Worker{
		cfg:      cfg,
		token:    token,
		executor: exec,
		log:      log,
		cancels:  make(map[string]context.CancelFunc),
		gs:       grpc.NewServer(),
	}
}

func (w *Worker) Run(ctx context.Context) error {
	chronosv1.RegisterWorkerServiceServer(w.gs, w)

	ln, err := net.Listen("tcp", w.cfg.ListenAddr)
	if err != nil {
		return err
	}
	w.log.Info("worker gRPC listening", "addr", w.cfg.ListenAddr, "id", w.cfg.ID)

	errCh := make(chan error, 2)
	go func() { errCh <- w.gs.Serve(ln) }()

	go func() {
		if err := w.session(ctx); err != nil && ctx.Err() == nil {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		w.gs.GracefulStop()
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (w *Worker) session(ctx context.Context) error {
	var conn *grpc.ClientConn
	var client chronosv1.SchedulerServiceClient

	dial := func() error {
		dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		c, err := grpc.DialContext(dctx, w.cfg.SchedulerAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		if err != nil {
			return err
		}
		if conn != nil {
			_ = conn.Close()
		}
		conn = c
		client = chronosv1.NewSchedulerServiceClient(conn)
		resp, err := client.Register(ctx, &chronosv1.RegisterRequest{
			WorkerId:   w.cfg.ID,
			Address:    w.cfg.AdvertiseAddr,
			Capacity:   int32(w.cfg.Capacity),
			CapBuiltin: true,
			CapWebhook: true,
			Token:      w.token,
		})
		if err != nil {
			return err
		}
		if resp != nil && !resp.Ok {
			return errRegister{msg: resp.Message}
		}
		w.log.Info("registered with scheduler", "scheduler", w.cfg.SchedulerAddr)
		return nil
	}

	for {
		if err := dial(); err != nil {
			w.log.Warn("register failed, retrying", "err", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
				continue
			}
		}
		break
	}
	defer func() {
		if client != nil {
			_, _ = client.Deregister(context.Background(), &chronosv1.DeregisterRequest{
				WorkerId: w.cfg.ID,
				Token:    w.token,
			})
		}
		if conn != nil {
			_ = conn.Close()
		}
	}()

	t := time.NewTicker(w.cfg.HeartbeatEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			resp, err := client.Heartbeat(ctx, &chronosv1.HeartbeatRequest{
				WorkerId:   w.cfg.ID,
				ActiveJobs: w.active.Load(),
				Capacity:   int32(w.cfg.Capacity),
				Token:      w.token,
			})
			if err != nil {
				w.log.Warn("heartbeat failed", "err", err)
				_ = dial()
				continue
			}
			if resp != nil {
				for _, jobID := range resp.GetCancelJobIds() {
					w.cancelLocal(jobID)
				}
			}
		}
	}
}

func (w *Worker) cancelLocal(jobID string) {
	w.mu.Lock()
	cancel, ok := w.cancels[jobID]
	w.mu.Unlock()
	if ok {
		cancel()
		w.log.Info("cancel applied", "job_id", jobID)
	}
}

type errRegister struct{ msg string }

func (e errRegister) Error() string { return "register: " + e.msg }

func (w *Worker) ExecuteJob(ctx context.Context, req *chronosv1.ExecuteJobRequest) (*chronosv1.ExecuteJobResponse, error) {
	if int(w.active.Load()) >= w.cfg.Capacity {
		return &chronosv1.ExecuteJobResponse{Accepted: false, Message: "at capacity"}, nil
	}
	w.active.Add(1)

	timeout := time.Duration(req.GetTimeoutMs()) * time.Millisecond
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	jobCtx, cancel := context.WithTimeout(context.Background(), timeout)
	w.mu.Lock()
	w.cancels[req.GetJobId()] = cancel
	w.mu.Unlock()

	go func() {
		defer w.active.Add(-1)
		defer cancel()
		defer func() {
			w.mu.Lock()
			delete(w.cancels, req.GetJobId())
			w.mu.Unlock()
		}()

		result := w.executor.Execute(jobCtx, domain.JobType(req.GetType()), req.GetPayload())
		w.report(req, result)
	}()

	return &chronosv1.ExecuteJobResponse{Accepted: true}, nil
}

func (w *Worker) report(req *chronosv1.ExecuteJobRequest, result domain.ExecutionResult) {
	dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(dctx, w.cfg.SchedulerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		w.log.Error("report dial failed", "err", err)
		return
	}
	defer conn.Close()
	client := chronosv1.NewSchedulerServiceClient(conn)
	reason := string(result.Reason)
	if reason == "" {
		if result.Success {
			reason = string(domain.ReasonSuccess)
		} else {
			reason = string(domain.ReasonFailed)
		}
	}
	_, err = client.ReportResult(dctx, &chronosv1.ReportResultRequest{
		JobId:       req.GetJobId(),
		ExecutionId: req.GetExecutionId(),
		WorkerId:    w.cfg.ID,
		Attempt:     req.GetAttempt(),
		Success:     result.Success,
		Output:      result.Output,
		Error:       result.Error,
		Retryable:   result.Retryable,
		DurationMs:  result.Duration.Milliseconds(),
		Token:       w.token,
		Reason:      reason,
	})
	if err != nil {
		w.log.Error("report result failed", "job_id", req.GetJobId(), "err", err)
		return
	}
	w.log.Info("job finished", "job_id", req.GetJobId(), "success", result.Success, "reason", reason)
}

func (w *Worker) CancelJob(ctx context.Context, req *chronosv1.CancelJobRequest) (*chronosv1.CancelJobResponse, error) {
	w.mu.Lock()
	_, existed := w.cancels[req.GetJobId()]
	w.mu.Unlock()
	w.cancelLocal(req.GetJobId())
	return &chronosv1.CancelJobResponse{Cancelled: existed}, nil
}

func (w *Worker) Health(ctx context.Context, _ *chronosv1.HealthRequest) (*chronosv1.HealthResponse, error) {
	return &chronosv1.HealthResponse{
		WorkerId:   w.cfg.ID,
		Capacity:   int32(w.cfg.Capacity),
		ActiveJobs: w.active.Load(),
		Healthy:    true,
	}, nil
}
