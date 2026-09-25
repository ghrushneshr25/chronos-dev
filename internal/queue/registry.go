package queue

import (
	"fmt"
	"sync"

	"github.com/ghrushneshr25/chronos-dev/internal/domain"
	"github.com/ghrushneshr25/chronos-dev/internal/queue/kafka"
	"github.com/ghrushneshr25/chronos-dev/internal/queue/memory"
)

var (
	mu        sync.RWMutex
	factories = map[string]domain.QueueFactory{}
)

func init() {
	Register("kafka", func(cfg domain.QueueConfig) (domain.Queue, error) {
		return kafka.New(kafka.Config{
			Brokers: cfg.KafkaBrokers,
			Topic:   cfg.KafkaTopic,
			Group:   cfg.KafkaGroup,
		})
	})
	Register("memory", func(cfg domain.QueueConfig) (domain.Queue, error) {
		return memory.New(), nil
	})
}

// Register adds a queue backend factory (call from backend init or tests).
func Register(name string, factory domain.QueueFactory) {
	mu.Lock()
	defer mu.Unlock()
	factories[name] = factory
}

// New creates a Queue for the configured backend.
// CHRONOS_QUEUE_BACKEND: kafka (default) | memory | postgres | redis (when registered)
func New(cfg domain.QueueConfig) (domain.Queue, error) {
	backend := cfg.Backend
	if backend == "" {
		backend = "kafka"
	}
	mu.RLock()
	factory, ok := factories[backend]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown queue backend %q (registered: %v)", backend, List())
	}
	return factory(cfg)
}

// List returns registered backend names.
func List() []string {
	mu.RLock()
	defer mu.RUnlock()
	names := make([]string, 0, len(factories))
	for n := range factories {
		names = append(names, n)
	}
	return names
}
