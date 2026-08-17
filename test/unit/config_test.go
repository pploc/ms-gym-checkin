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

func TestGivenMissingKMSKeyID_WhenLoadingConfig_ThenRejectsStartup(t *testing.T) {
	// Given
	setRequiredConfig(t)
	t.Setenv("KMS_KEY_ID", "")

	// When
	_, err := config.Load()

	// Then
	if err == nil {
		t.Fatal("expected missing KMS key ID error")
	}
}

func TestGivenKMSConfiguration_WhenLoadingConfig_ThenLoadsEndpointAndKeepsReflectionDisabled(t *testing.T) {
	// Given
	setRequiredConfig(t)
	t.Setenv("KMS_ENDPOINT_URL", "http://localstack:4566")

	// When
	cfg, err := config.Load()

	// Then
	if err != nil {
		t.Fatal("load KMS configuration failed")
	}
	if cfg.KMSEndpointURL != "http://localstack:4566" {
		t.Fatal("KMS endpoint was not loaded")
	}
	if cfg.GRPCReflection {
		t.Fatal("gRPC reflection must default to disabled")
	}
}

func setRequiredConfig(t *testing.T) {
	t.Helper()
	for key, value := range map[string]string{
		"DATABASE_URL":             "postgres://localhost/checkin",
		"AWS_REGION":               "us-east-1",
		"KMS_KEY_ID":               "alias/checkin-root",
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
