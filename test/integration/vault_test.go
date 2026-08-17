//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"testing"
	"time"

	vaultapi "github.com/hashicorp/vault/api"

	vaultadapter "github.com/pploc/ms-gym-checkin/internal/adapter/vault"
	"github.com/pploc/ms-gym-checkin/internal/config"
)

func TestGivenLiveTransit_WhenEncryptingDecryptingAndCheckingReadiness_ThenRoundTripsWithoutLoggingKeyMaterial(t *testing.T) {
	// Given
	address, token := vaultEnvironment(t)
	transit, err := vaultadapter.New(address, config.VaultAuth{Token: token}, "transit", "checkin-root")
	if err != nil {
		t.Fatal("create Vault Transit client failed")
	}
	plaintext := make([]byte, 32)
	if _, err := rand.Read(plaintext); err != nil {
		t.Fatal("generate test root key failed")
	}

	// When
	if err := transit.Ping(context.Background()); err != nil {
		t.Fatal("ping Vault Transit failed")
	}
	ciphertext, err := transit.Encrypt(context.Background(), plaintext)
	if err != nil {
		t.Fatal("encrypt test root key failed")
	}
	decrypted, err := transit.Decrypt(context.Background(), ciphertext)

	// Then
	if err != nil {
		t.Fatal("decrypt test root key failed")
	}
	if !bytes.Equal(decrypted, plaintext) {
		t.Fatal("decrypted value differs")
	}
}

func TestGivenTransitTokenWithoutTransitPolicy_WhenEncrypting_ThenDeniesAccess(t *testing.T) {
	// Given
	address, token := vaultEnvironment(t)
	client, err := vaultapi.NewClient(&vaultapi.Config{Address: address})
	if err != nil {
		t.Fatal("create Vault API client failed")
	}
	client.SetToken(token)
	secret, err := client.Logical().Write("auth/token/create", map[string]any{"policies": []string{"default"}, "ttl": "1m"})
	if err != nil || secret == nil || secret.Auth == nil || secret.Auth.ClientToken == "" {
		t.Fatal("create restricted token")
	}
	t.Cleanup(func() {
		_, _ = client.Logical().Write("auth/token/revoke-self", map[string]any{"token": secret.Auth.ClientToken})
	})
	transit, err := vaultadapter.New(address, config.VaultAuth{Token: secret.Auth.ClientToken}, "transit", "checkin-root")
	if err != nil {
		t.Fatal("create restricted Vault Transit client failed")
	}

	// When
	_, err = transit.Encrypt(context.Background(), []byte("checkin-test"))

	// Then
	if err == nil {
		t.Fatal("expected transit policy denial")
	}
}

func vaultEnvironment(t *testing.T) (string, string) {
	t.Helper()
	address, token := os.Getenv("VAULT_ADDR"), os.Getenv("VAULT_TOKEN")
	if address == "" || token == "" {
		t.Skip("VAULT_ADDR and VAULT_TOKEN not set; run make start-env")
	}
	return address, token
}

func TestGivenUnavailableVault_WhenPinging_ThenFailsClosed(t *testing.T) {
	// Given
	transit, err := vaultadapter.New("http://127.0.0.1:1", config.VaultAuth{Token: "not-used"}, "transit", "checkin-root")
	if err != nil {
		t.Fatal("create unavailable Vault Transit client failed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// When
	err = transit.Ping(ctx)

	// Then
	if err == nil {
		t.Fatal("expected unavailable Vault error")
	}
}
