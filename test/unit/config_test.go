package unit

import (
	"testing"

	"github.com/pploc/ms-gym-checkin/internal/config"
)

func TestGivenInvalidDuration_WhenLoadingConfig_ThenRejectsStartup(t *testing.T) {
	// Given
	setRequiredConfig(t)
	t.Setenv("READINESS_TIMEOUT", "not-a-duration")

	// When
	_, err := config.Load()

	// Then
	if err == nil {
		t.Fatal("expected invalid duration error")
	}
}

func TestGivenBothVaultAuthModes_WhenLoadingConfig_ThenRejectsStartup(t *testing.T) {
	// Given
	setRequiredConfig(t)
	t.Setenv("VAULT_TOKEN", "local")
	t.Setenv("VAULT_KUBERNETES_AUTH_ROLE", "checkin")

	// When
	_, err := config.Load()

	// Then
	if err == nil {
		t.Fatal("expected mutually exclusive Vault auth error")
	}
}

func TestGivenKubernetesVaultAuth_WhenLoadingConfig_ThenAcceptsProjectedIdentity(t *testing.T) {
	// Given
	setRequiredConfig(t)
	t.Setenv("VAULT_TOKEN", "")
	t.Setenv("VAULT_KUBERNETES_AUTH_ROLE", "checkin")

	// When
	cfg, err := config.Load()

	// Then
	if err != nil {
		t.Fatal("load Kubernetes Vault configuration failed")
	}
	if cfg.VaultAuth.KubernetesRole != "checkin" {
		t.Fatal("Kubernetes Vault role was not loaded")
	}
	if cfg.GRPCReflection {
		t.Fatal("gRPC reflection must default to disabled")
	}
}

func setRequiredConfig(t *testing.T) {
	t.Helper()
	for key, value := range map[string]string{
		"DATABASE_URL":             "postgres://localhost/checkin",
		"VAULT_ADDR":               "http://vault:8200",
		"VAULT_TOKEN":              "local",
		"VAULT_KEY_REFERENCE":      "checkin-root",
		"MEMBER_GRPC_ADDR":         "member:50051",
		"MEMBER_GRPC_CERT":         "/tmp/member.crt",
		"MEMBER_GRPC_KEY":          "/tmp/member.key",
		"MEMBER_GRPC_CA":           "/tmp/ca.crt",
		"PLANS_GRPC_ADDR":          "plans:50051",
		"PLANS_GRPC_CERT":          "/tmp/plans.crt",
		"PLANS_GRPC_KEY":           "/tmp/plans.key",
		"PLANS_GRPC_CA":            "/tmp/ca.crt",
		"CHECKIN_GRPC_SERVER_CERT": "/tmp/checkin.crt",
		"CHECKIN_GRPC_SERVER_KEY":  "/tmp/checkin.key",
		"CHECKIN_GRPC_CLIENT_CA":   "/tmp/ca.crt",
		"KAFKA_BROKERS":            "kafka:9092",
		"SCHEMA_REGISTRY_URL":      "http://registry:8081",
	} {
		t.Setenv(key, value)
	}
}
