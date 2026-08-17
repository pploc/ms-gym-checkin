//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"os"
	"testing"
	"time"

	kmsadapter "github.com/pploc/ms-gym-checkin/internal/adapter/kms"
)

func TestGivenLiveKMS_WhenEncryptingDecryptingAndCheckingReadiness_ThenRoundTripsWithoutLoggingKeyMaterial(t *testing.T) {
	// Given
	region, keyID, endpointURL := kmsEnvironment(t)
	protector, err := kmsadapter.New(context.Background(), region, keyID, endpointURL)
	if err != nil {
		t.Fatal("create KMS protector failed")
	}
	plaintext := make([]byte, 32)
	if _, err := rand.Read(plaintext); err != nil {
		t.Fatal("generate test root key failed")
	}

	// When
	if err := protector.Ping(context.Background()); err != nil {
		t.Fatal("ping KMS failed")
	}
	ciphertext, err := protector.Encrypt(context.Background(), plaintext)
	if err != nil {
		t.Fatal("encrypt test root key failed")
	}
	decrypted, err := protector.Decrypt(context.Background(), protector.KeyReference(), ciphertext)

	// Then
	if err != nil {
		t.Fatal("decrypt test root key failed")
	}
	if _, err := base64.StdEncoding.DecodeString(ciphertext); err != nil || ciphertext == string(plaintext) {
		t.Fatal("ciphertext was not opaque base64")
	}
	if !bytes.Equal(decrypted, plaintext) {
		t.Fatal("decrypted value differs")
	}
}

func TestGivenUnknownKMSKey_WhenCreatingProtector_ThenFailsClosed(t *testing.T) {
	// Given
	region, _, endpointURL := kmsEnvironment(t)

	// When
	_, err := kmsadapter.New(context.Background(), region, "alias/missing-checkin-root", endpointURL)

	// Then
	if err == nil {
		t.Fatal("expected unknown KMS key error")
	}
}

func TestGivenUnavailableKMS_WhenCreatingProtector_ThenFailsClosed(t *testing.T) {
	// Given
	region, keyID, _ := kmsEnvironment(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// When
	_, err := kmsadapter.New(ctx, region, keyID, "http://127.0.0.1:1")

	// Then
	if err == nil {
		t.Fatal("expected unavailable KMS error")
	}
}

func kmsEnvironment(t *testing.T) (string, string, string) {
	t.Helper()
	region, keyID, endpointURL := os.Getenv("AWS_REGION"), os.Getenv("KMS_KEY_ID"), os.Getenv("KMS_ENDPOINT_URL")
	if region == "" || keyID == "" || endpointURL == "" {
		t.Skip("AWS_REGION, KMS_KEY_ID, and KMS_ENDPOINT_URL not set; run make start-env")
	}
	return region, keyID, endpointURL
}
