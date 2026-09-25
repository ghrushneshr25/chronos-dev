package app

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/ghrushneshr25/chronos-dev/internal/auth"
	"github.com/ghrushneshr25/chronos-dev/internal/config"
	"github.com/ghrushneshr25/chronos-dev/internal/domain"
	"github.com/ghrushneshr25/chronos-dev/internal/events"
	"github.com/ghrushneshr25/chronos-dev/internal/execution"
	"github.com/ghrushneshr25/chronos-dev/internal/ports"
	"github.com/ghrushneshr25/chronos-dev/internal/queue"
	"github.com/ghrushneshr25/chronos-dev/internal/queue/kafka"
	"github.com/ghrushneshr25/chronos-dev/internal/queue/memory"
	"github.com/ghrushneshr25/chronos-dev/internal/store/postgres"
	"github.com/ghrushneshr25/nexus"
)

func JobStoreContract() nexus.Contract[ports.JobStore] {
	return nexus.ContractOf[ports.JobStore]()
}

func EventPublisherContract() nexus.Contract[ports.EventPublisher] {
	return nexus.ContractOf[ports.EventPublisher]()
}

func QueueContract() nexus.Contract[domain.Queue] {
	return nexus.ContractOf[domain.Queue]()
}

func KafkaQueueContract() nexus.Contract[domain.Queue] {
	return nexus.NamedContractOf[domain.Queue]("kafka")
}

func MemoryQueueContract() nexus.Contract[domain.Queue] {
	return nexus.NamedContractOf[domain.Queue]("memory")
}

func JobExecutorContract() nexus.Contract[ports.JobExecutor] {
	return nexus.ContractOf[ports.JobExecutor]()
}

// Register loads config and wires Chronos into nexus.
// Queue backends are named services; CHRONOS_QUEUE_BACKEND selects the default.
func Register(serviceName string) (*config.Config, *auth.Authenticator, error) {
	cfg, err := config.Load(serviceName)
	if err != nil {
		return nil, nil, err
	}

	nexus.MustDeclareValue(cfg)

	nexus.MustDeclare(newJobStore)
	nexus.MustDeclare(newEventPublisher)
	nexus.MustDeclare(newJobExecutor)
	nexus.MustDeclareNamed("kafka", newKafkaQueue)
	nexus.MustDeclareNamed("memory", newMemoryQueue)
	nexus.MustDeclare(newActiveQueue)

	if err := nexus.Validate(); err != nil {
		return nil, nil, fmt.Errorf("nexus validate: %w", err)
	}

	a := auth.New(cfg.Auth.APIKeys, cfg.Auth.WorkerTokens, cfg.Auth.Disabled)
	return cfg, a, nil
}

func newJobStore(cfg *config.Config) (ports.JobStore, error) {
	return postgres.New(
		cfg.Postgres.DSN,
		cfg.Postgres.MaxOpenConns,
		cfg.Postgres.MaxIdleConns,
		cfg.Postgres.ConnMaxLifetime,
	)
}

func newEventPublisher(store ports.JobStore, cfg *config.Config) ports.EventPublisher {
	ps, ok := store.(*postgres.Store)
	if !ok {
		return events.NoopPublisher{}
	}
	return events.NewPublisher(ps, cfg.Scheduler.ID)
}

func newJobExecutor(cfg *config.Config) ports.JobExecutor {
	return execution.NewExecutor(cfg.Worker.HTTPClientTimeout)
}

func newKafkaQueue(cfg *config.Config) (domain.Queue, error) {
	return kafka.New(kafka.Config{
		Brokers: cfg.Queue.KafkaBrokers,
		Topic:   cfg.Queue.KafkaTopic,
		Group:   cfg.Queue.KafkaGroup,
	})
}

func newMemoryQueue(cfg *config.Config) (domain.Queue, error) {
	return memory.New(), nil
}

func newActiveQueue(cfg *config.Config) (domain.Queue, error) {
	backend := cfg.Queue.Backend
	if backend == "" {
		backend = "kafka"
	}
	switch backend {
	case "kafka":
		return nexus.GetNamed(KafkaQueueContract)
	case "memory":
		return nexus.GetNamed(MemoryQueueContract)
	default:
		return queue.New(domain.QueueConfig{
			Backend:      backend,
			KafkaBrokers: cfg.Queue.KafkaBrokers,
			KafkaTopic:   cfg.Queue.KafkaTopic,
			KafkaGroup:   cfg.Queue.KafkaGroup,
		})
	}
}

func Logger(service string) *slog.Logger {
	level := slog.LevelInfo
	switch os.Getenv("CHRONOS_LOG_LEVEL") {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})).
		With("service", service)
}
