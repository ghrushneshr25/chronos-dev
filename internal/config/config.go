package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ServiceName string
	Environment string
	LogLevel    string

	API       APIConfig
	Scheduler SchedulerConfig
	Worker    WorkerConfig
	Postgres  PostgresConfig
	Queue     QueueConfig
	Kafka     KafkaConfig
	ClickHouse ClickHouseConfig
	Auth      AuthConfig
	OTel      OTelConfig
}

type APIConfig struct {
	HTTPAddr string
	GRPCAddr string
}

type SchedulerConfig struct {
	ID                string
	PollInterval      time.Duration
	AssignBatchSize   int
	HeartbeatTimeout  time.Duration
	SuspectGracePeriod time.Duration
	GRPCAddr          string // address workers dial to register (advertised)
	ListenAddr        string // where scheduler gRPC server listens
}

type WorkerConfig struct {
	ID            string
	ListenAddr    string
	AdvertiseAddr string
	Capacity      int
	HeartbeatEvery time.Duration
	SchedulerAddr string
	HTTPClientTimeout time.Duration
}

type PostgresConfig struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

type QueueConfig struct {
	Backend      string
	KafkaTopic   string
	KafkaGroup   string
	KafkaBrokers []string
}

type KafkaConfig struct {
	Brokers            []string
	JobEventsTopic     string
	WorkerEventsTopic  string
	SchedulerEventsTopic string
	ConsumerGroup      string
}

type ClickHouseConfig struct {
	DSN      string
	Database string
}

type AuthConfig struct {
	APIKeys          []string
	WorkerTokens     []string
	Disabled         bool
}

type OTelConfig struct {
	Endpoint    string
	ServiceName string
	Enabled     bool
}

func Load(service string) (*Config, error) {
	cfg := &Config{
		ServiceName: service,
		Environment: getEnv("CHRONOS_ENV", "development"),
		LogLevel:    getEnv("CHRONOS_LOG_LEVEL", "info"),
		API: APIConfig{
			HTTPAddr: getEnv("CHRONOS_API_HTTP_ADDR", ":8080"),
			GRPCAddr: getEnv("CHRONOS_API_GRPC_ADDR", ":9090"),
		},
		Scheduler: SchedulerConfig{
			ID:                 getEnv("CHRONOS_SCHEDULER_ID", "scheduler-1"),
			PollInterval:       getDuration("CHRONOS_SCHEDULER_POLL_INTERVAL", 500*time.Millisecond),
			AssignBatchSize:    getInt("CHRONOS_SCHEDULER_ASSIGN_BATCH", 50),
			HeartbeatTimeout:   getDuration("CHRONOS_WORKER_HEARTBEAT_TIMEOUT", 15*time.Second),
			SuspectGracePeriod: getDuration("CHRONOS_WORKER_SUSPECT_GRACE", 10*time.Second),
			GRPCAddr:           getEnv("CHRONOS_SCHEDULER_ADVERTISE_ADDR", "scheduler:9091"),
			ListenAddr:         getEnv("CHRONOS_SCHEDULER_LISTEN_ADDR", ":9091"),
		},
		Worker: WorkerConfig{
			ID:                getEnv("CHRONOS_WORKER_ID", ""),
			ListenAddr:        getEnv("CHRONOS_WORKER_LISTEN_ADDR", ":9092"),
			AdvertiseAddr:     getEnv("CHRONOS_WORKER_ADVERTISE_ADDR", ""),
			Capacity:          getInt("CHRONOS_WORKER_CAPACITY", 10),
			HeartbeatEvery:    getDuration("CHRONOS_WORKER_HEARTBEAT_INTERVAL", 5*time.Second),
			SchedulerAddr:     getEnv("CHRONOS_SCHEDULER_ADDR", "scheduler:9091"),
			HTTPClientTimeout: getDuration("CHRONOS_WORKER_HTTP_TIMEOUT", 30*time.Second),
		},
		Postgres: PostgresConfig{
			DSN:             getEnv("CHRONOS_POSTGRES_DSN", "postgres://chronos:chronos@localhost:5432/chronos?sslmode=disable"),
			MaxOpenConns:    getInt("CHRONOS_POSTGRES_MAX_OPEN", 25),
			MaxIdleConns:    getInt("CHRONOS_POSTGRES_MAX_IDLE", 5),
			ConnMaxLifetime: getDuration("CHRONOS_POSTGRES_CONN_LIFETIME", time.Hour),
		},
		Queue: QueueConfig{
			Backend:      getEnv("CHRONOS_QUEUE_BACKEND", "kafka"),
			KafkaTopic:   getEnv("CHRONOS_QUEUE_KAFKA_TOPIC", "chronos.jobs"),
			KafkaGroup:   getEnv("CHRONOS_QUEUE_KAFKA_GROUP", "chronos-scheduler"),
			KafkaBrokers: splitCSV(getEnv("CHRONOS_KAFKA_BROKERS", "localhost:9092")),
		},
		Kafka: KafkaConfig{
			Brokers:              splitCSV(getEnv("CHRONOS_KAFKA_BROKERS", "localhost:9092")),
			JobEventsTopic:       getEnv("CHRONOS_KAFKA_JOB_EVENTS_TOPIC", "chronos.job-events"),
			WorkerEventsTopic:    getEnv("CHRONOS_KAFKA_WORKER_EVENTS_TOPIC", "chronos.worker-events"),
			SchedulerEventsTopic: getEnv("CHRONOS_KAFKA_SCHEDULER_EVENTS_TOPIC", "chronos.scheduler-events"),
			ConsumerGroup:        getEnv("CHRONOS_KAFKA_CONSUMER_GROUP", "chronos-analytics"),
		},
		ClickHouse: ClickHouseConfig{
			DSN:      getEnv("CHRONOS_CLICKHOUSE_DSN", "clickhouse://chronos:chronos@localhost:9000/chronos"),
			Database: getEnv("CHRONOS_CLICKHOUSE_DATABASE", "chronos"),
		},
		Auth: AuthConfig{
			APIKeys:      splitCSV(getEnv("CHRONOS_API_KEYS", "dev-api-key-change-me")),
			WorkerTokens: splitCSV(getEnv("CHRONOS_WORKER_TOKENS", "dev-worker-token-change-me")),
			Disabled:     getBool("CHRONOS_AUTH_DISABLED", false),
		},
		OTel: OTelConfig{
			Endpoint:    getEnv("CHRONOS_OTEL_ENDPOINT", "localhost:4317"),
			ServiceName: service,
			Enabled:     getBool("CHRONOS_OTEL_ENABLED", false),
		},
	}

	if cfg.Worker.ID == "" {
		host, _ := os.Hostname()
		cfg.Worker.ID = fmt.Sprintf("worker-%s-%d", host, os.Getpid())
	}
	if cfg.Worker.AdvertiseAddr == "" {
		cfg.Worker.AdvertiseAddr = cfg.Worker.ListenAddr
	}

	return cfg, nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func getBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func getDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
