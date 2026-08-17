package config

import (
	"fmt"
	"os"
	"strings"
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

type VaultAuth struct {
	Token             string
	KubernetesRole    string
	KubernetesJWTFile string
	KubernetesMount   string
}

type Config struct {
	GRPCAddr          string
	HTTPAddr          string
	DatabaseURL       string
	Member            MTLSClient
	Plans             MTLSClient
	VaultAddress      string
	VaultAuth         VaultAuth
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
	GRPCReflection    bool
}

func Load() (Config, error) {
	member, err := client("MEMBER", "ms-gym-member")
	if err != nil {
		return Config{}, err
	}
	plans, err := client("PLANS", "ms-gym-plans")
	if err != nil {
		return Config{}, err
	}
	relayInterval, err := positiveDuration("OUTBOX_RELAY_INTERVAL", 2*time.Second)
	if err != nil {
		return Config{}, err
	}
	shutdownTimeout, err := positiveDuration("SHUTDOWN_TIMEOUT", 15*time.Second)
	if err != nil {
		return Config{}, err
	}
	readinessTimeout, err := positiveDuration("READINESS_TIMEOUT", 2*time.Second)
	if err != nil {
		return Config{}, err
	}
	reflection, err := boolean("GRPC_REFLECTION", false)
	if err != nil {
		return Config{}, err
	}
	cfg := Config{
		GRPCAddr:     env("GRPC_ADDR", ":50051"),
		HTTPAddr:     env("HTTP_ADDR", ":8080"),
		DatabaseURL:  os.Getenv("DATABASE_URL"),
		Member:       member,
		Plans:        plans,
		VaultAddress: os.Getenv("VAULT_ADDR"),
		VaultAuth: VaultAuth{
			Token:             os.Getenv("VAULT_TOKEN"),
			KubernetesRole:    os.Getenv("VAULT_KUBERNETES_AUTH_ROLE"),
			KubernetesJWTFile: env("VAULT_KUBERNETES_JWT_FILE", "/var/run/secrets/vault/token"),
			KubernetesMount:   env("VAULT_KUBERNETES_AUTH_MOUNT", "kubernetes"),
		},
		VaultTransitMount: env("VAULT_TRANSIT_MOUNT", "transit"),
		VaultKeyReference: os.Getenv("VAULT_KEY_REFERENCE"),
		KafkaBrokers:      os.Getenv("KAFKA_BROKERS"),
		SchemaRegistryURL: os.Getenv("SCHEMA_REGISTRY_URL"),
		ServiceName:       env("SERVICE_NAME", "ms-gym-checkin"),
		RelayInterval:     relayInterval,
		ShutdownTimeout:   shutdownTimeout,
		ReadinessTimeout:  readinessTimeout,
		GRPCServerCert:    os.Getenv("CHECKIN_GRPC_SERVER_CERT"),
		GRPCServerKey:     os.Getenv("CHECKIN_GRPC_SERVER_KEY"),
		GRPCClientCA:      os.Getenv("CHECKIN_GRPC_CLIENT_CA"),
		GRPCReflection:    reflection,
	}
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}
	if cfg.VaultAddress == "" || cfg.VaultKeyReference == "" {
		return Config{}, fmt.Errorf("VAULT_ADDR and VAULT_KEY_REFERENCE are required")
	}
	if (cfg.VaultAuth.Token == "") == (cfg.VaultAuth.KubernetesRole == "") {
		return Config{}, fmt.Errorf("exactly one of VAULT_TOKEN or VAULT_KUBERNETES_AUTH_ROLE is required")
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
	if strings.TrimSpace(cfg.KafkaBrokers) == "" || strings.TrimSpace(cfg.SchemaRegistryURL) == "" {
		return Config{}, fmt.Errorf("KAFKA_BROKERS and SCHEMA_REGISTRY_URL are required")
	}
	return cfg, nil
}

func client(prefix, defaultName string) (MTLSClient, error) {
	deadline, err := positiveDuration(prefix+"_GRPC_DEADLINE", 3*time.Second)
	if err != nil {
		return MTLSClient{}, err
	}
	return MTLSClient{Address: os.Getenv(prefix + "_GRPC_ADDR"), CertFile: os.Getenv(prefix + "_GRPC_CERT"), KeyFile: os.Getenv(prefix + "_GRPC_KEY"), CAFile: os.Getenv(prefix + "_GRPC_CA"), ServerName: env(prefix+"_GRPC_SERVER_NAME", defaultName), Deadline: deadline}, nil
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

func positiveDuration(key string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", key)
	}
	return parsed, nil
}

func boolean(key string, fallback bool) (bool, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	switch strings.ToLower(value) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("%s must be true or false", key)
	}
}
