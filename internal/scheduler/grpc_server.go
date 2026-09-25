package scheduler

import (
	"context"
	"log/slog"
	"net"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/auth"
	"github.com/ghrushneshr25/chronos-dev/internal/domain"
	chronosv1 "github.com/ghrushneshr25/chronos-dev/proto/chronos/v1"
	"google.golang.org/grpc"
)

type GRPCServer struct {
	chronosv1.UnimplementedSchedulerServiceServer
	sched *Service
	auth  *auth.Authenticator
	log   *slog.Logger
	gs    *grpc.Server
}

func NewGRPCServer(sched *Service, a *auth.Authenticator, log *slog.Logger) *GRPCServer {
	if log == nil {
		log = slog.Default()
	}
	return &GRPCServer{sched: sched, auth: a, log: log, gs: grpc.NewServer()}
}

func (g *GRPCServer) Serve(ctx context.Context, addr string) error {
	chronosv1.RegisterSchedulerServiceServer(g.gs, g)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	g.log.Info("scheduler gRPC listening", "addr", addr)
	errCh := make(chan error, 1)
	go func() { errCh <- g.gs.Serve(ln) }()
	select {
	case <-ctx.Done():
		g.gs.GracefulStop()
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (g *GRPCServer) Register(ctx context.Context, req *chronosv1.RegisterRequest) (*chronosv1.RegisterResponse, error) {
	if !g.auth.ValidWorkerToken(req.GetToken()) {
		return &chronosv1.RegisterResponse{Ok: false, Message: "unauthorized"}, nil
	}
	w := &domain.Worker{
		ID:       req.GetWorkerId(),
		Address:  req.GetAddress(),
		Capacity: int(req.GetCapacity()),
		Capabilities: domain.WorkerCapabilities{
			Builtin: req.GetCapBuiltin(),
			Webhook: req.GetCapWebhook(),
		},
		Labels: req.GetLabels(),
	}
	if err := g.sched.RegisterWorker(ctx, w); err != nil {
		return &chronosv1.RegisterResponse{Ok: false, Message: err.Error()}, nil
	}
	return &chronosv1.RegisterResponse{Ok: true, Message: "registered"}, nil
}

func (g *GRPCServer) Heartbeat(ctx context.Context, req *chronosv1.HeartbeatRequest) (*chronosv1.HeartbeatResponse, error) {
	if !g.auth.ValidWorkerToken(req.GetToken()) {
		return &chronosv1.HeartbeatResponse{Ok: false}, nil
	}
	cancelIDs, err := g.sched.Heartbeat(ctx, req.GetWorkerId(), int(req.GetActiveJobs()), int(req.GetCapacity()))
	if err != nil {
		return &chronosv1.HeartbeatResponse{Ok: false}, err
	}
	return &chronosv1.HeartbeatResponse{Ok: true, CancelJobIds: cancelIDs}, nil
}

func (g *GRPCServer) Deregister(ctx context.Context, req *chronosv1.DeregisterRequest) (*chronosv1.DeregisterResponse, error) {
	if !g.auth.ValidWorkerToken(req.GetToken()) {
		return &chronosv1.DeregisterResponse{Ok: false}, nil
	}
	_ = g.sched.Deregister(ctx, req.GetWorkerId())
	return &chronosv1.DeregisterResponse{Ok: true}, nil
}

func (g *GRPCServer) ReportResult(ctx context.Context, req *chronosv1.ReportResultRequest) (*chronosv1.ReportResultResponse, error) {
	if !g.auth.ValidWorkerToken(req.GetToken()) {
		return &chronosv1.ReportResultResponse{Ok: false}, nil
	}
	res := domain.ExecutionResult{
		Success:   req.GetSuccess(),
		Output:    req.GetOutput(),
		Error:     req.GetError(),
		Retryable: req.GetRetryable(),
		Duration:  time.Duration(req.GetDurationMs()) * time.Millisecond,
		Reason:    domain.ResultReason(req.GetReason()),
	}
	if err := g.sched.HandleResult(ctx, req.GetJobId(), req.GetExecutionId(), req.GetWorkerId(), int(req.GetAttempt()), res); err != nil {
		return &chronosv1.ReportResultResponse{Ok: false}, err
	}
	return &chronosv1.ReportResultResponse{Ok: true}, nil
}
