package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type MTLSClient struct {
	Address    string
	CertFile   string
	KeyFile    string
	CAFile     string
	ServerName string
	Deadline   time.Duration
}

type Config struct {
	GRPCAddr          string
	HTTPAddr          string
	DatabaseURL       string
	Member            MTLSClient
	Plans             MTLSClient
	VaultAddress      string
	VaultToken        string
	VaultTransitMount string
	VaultKeyReference string
	KafkaBrokers      string
	SchemaRegistryURL string
	ServiceName       string
	RelayInterval     time.Duration
	ShutdownTimeout   time.Duration
	ReadinessTimeout  time.Duration
	GRPCServerCert    string
	GRPCServerKey     string
	GRPCClientCA      string
}

func Load() (Config, error) {
	cfg := Config{
		GRPCAddr:          env("GRPC_ADDR", ":50051"),
		HTTPAddr:          env("HTTP_ADDR", ":8080"),
		DatabaseURL:       os.Getenv("DATABASE_URL"),
		Member:            client("MEMBER", "ms-gym-member"),
		Plans:             client("PLANS", "ms-gym-plans"),
		VaultAddress:      os.Getenv("VAULT_ADDR"),
		VaultToken:        os.Getenv("VAULT_TOKEN"),
		VaultTransitMount: env("VAULT_TRANSIT_MOUNT", "transit"),
		VaultKeyReference: os.Getenv("VAULT_KEY_REFERENCE"),
		KafkaBrokers:      env("KAFKA_BROKERS", "localhost:9092"),
		SchemaRegistryURL: env("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		ServiceName:       env("SERVICE_NAME", "ms-gym-checkin"),
		RelayInterval:     duration("OUTBOX_RELAY_INTERVAL", 2*time.Second),
		ShutdownTimeout:   duration("SHUTDOWN_TIMEOUT", 15*time.Second),
		ReadinessTimeout:  duration("READINESS_TIMEOUT", 2*time.Second),
		GRPCServerCert:    os.Getenv("CHECKIN_GRPC_SERVER_CERT"),
		GRPCServerKey:     os.Getenv("CHECKIN_GRPC_SERVER_KEY"),
		GRPCClientCA:      os.Getenv("CHECKIN_GRPC_CLIENT_CA"),
	}
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}
	if cfg.VaultAddress == "" || cfg.VaultKeyReference == "" {
		return Config{}, fmt.Errorf("VAULT_ADDR and VAULT_KEY_REFERENCE are required")
	}
	if cfg.VaultToken == "" {
		return Config{}, fmt.Errorf("VAULT_TOKEN is required")
	}
	if err := validateClient("MEMBER", cfg.Member); err != nil {
		return Config{}, err
	}
	if err := validateClient("PLANS", cfg.Plans); err != nil {
		return Config{}, err
	}
	if cfg.GRPCServerCert == "" || cfg.GRPCServerKey == "" || cfg.GRPCClientCA == "" {
		return Config{}, fmt.Errorf("CHECKIN_GRPC_SERVER_CERT, CHECKIN_GRPC_SERVER_KEY, and CHECKIN_GRPC_CLIENT_CA are required")
	}
	return cfg, nil
}

func client(prefix, defaultName string) MTLSClient {
	return MTLSClient{Address: os.Getenv(prefix + "_GRPC_ADDR"), CertFile: os.Getenv(prefix + "_GRPC_CERT"), KeyFile: os.Getenv(prefix + "_GRPC_KEY"), CAFile: os.Getenv(prefix + "_GRPC_CA"), ServerName: env(prefix+"_GRPC_SERVER_NAME", defaultName), Deadline: duration(prefix+"_GRPC_DEADLINE", 3*time.Second)}
}

func validateClient(name string, client MTLSClient) error {
	if client.Address == "" || client.CertFile == "" || client.KeyFile == "" || client.CAFile == "" {
		return fmt.Errorf("%s_GRPC_ADDR, %s_GRPC_CERT, %s_GRPC_KEY, and %s_GRPC_CA are required", name, name, name, name)
	}
	return nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
func duration(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	if parsed, err := time.ParseDuration(value); err == nil {
		return parsed
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return fallback
}
