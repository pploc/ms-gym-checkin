package vault

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	vaultapi "github.com/hashicorp/vault/api"

	"github.com/pploc/ms-gym-checkin/internal/config"
)

type Transit struct {
	client        *vaultapi.Client
	mount         string
	keyReference  string
	auth          config.VaultAuth
	authMu        sync.Mutex
	authenticated bool
	tokenExpires  time.Time
}

func New(address string, auth config.VaultAuth, mount, keyReference string) (*Transit, error) {
	cfg := vaultapi.DefaultConfig()
	cfg.Address = address
	client, err := vaultapi.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	transit := &Transit{client: client, mount: strings.Trim(mount, "/"), keyReference: keyReference, auth: auth}
	if auth.Token != "" {
		client.SetToken(auth.Token)
		transit.authenticated = true
	}
	return transit, nil
}

func (t *Transit) KeyReference() string { return t.keyReference }

func (t *Transit) Encrypt(ctx context.Context, plaintext []byte) (string, error) {
	if err := t.ensureAuthenticated(ctx); err != nil {
		return "", err
	}
	response, err := t.client.Logical().WriteWithContext(ctx, t.mount+"/encrypt/"+t.keyReference, map[string]any{"plaintext": base64.StdEncoding.EncodeToString(plaintext)})
	if err != nil {
		return "", err
	}
	if response == nil || response.Data == nil {
		return "", fmt.Errorf("vault returned no ciphertext")
	}
	ciphertext, ok := response.Data["ciphertext"].(string)
	if !ok || ciphertext == "" {
		return "", fmt.Errorf("vault returned invalid ciphertext")
	}
	return ciphertext, nil
}

func (t *Transit) Decrypt(ctx context.Context, ciphertext string) ([]byte, error) {
	if err := t.ensureAuthenticated(ctx); err != nil {
		return nil, err
	}
	response, err := t.client.Logical().WriteWithContext(ctx, t.mount+"/decrypt/"+t.keyReference, map[string]any{"ciphertext": ciphertext})
	if err != nil {
		return nil, err
	}
	if response == nil || response.Data == nil {
		return nil, fmt.Errorf("vault returned no plaintext")
	}
	encoded, ok := response.Data["plaintext"].(string)
	if !ok {
		return nil, fmt.Errorf("vault returned invalid plaintext")
	}
	return base64.StdEncoding.DecodeString(encoded)
}

func (t *Transit) Ping(ctx context.Context) error {
	if err := t.ensureAuthenticated(ctx); err != nil {
		return err
	}
	_, err := t.client.Auth().Token().LookupSelfWithContext(ctx)
	return err
}

func (t *Transit) ensureAuthenticated(ctx context.Context) error {
	if t.auth.Token != "" {
		return nil
	}
	t.authMu.Lock()
	defer t.authMu.Unlock()
	if t.authenticated && (t.tokenExpires.IsZero() || time.Until(t.tokenExpires) > time.Minute) {
		return nil
	}
	if t.auth.KubernetesRole == "" {
		return fmt.Errorf("Vault authentication is not configured")
	}
	jwt, err := os.ReadFile(t.auth.KubernetesJWTFile)
	if err != nil {
		return fmt.Errorf("read Vault Kubernetes identity: %w", err)
	}
	secret, err := t.client.Logical().WriteWithContext(ctx, "auth/"+strings.Trim(t.auth.KubernetesMount, "/")+"/login", map[string]any{
		"role": t.auth.KubernetesRole,
		"jwt":  strings.TrimSpace(string(jwt)),
	})
	if err != nil {
		return err
	}
	if secret == nil || secret.Auth == nil || secret.Auth.ClientToken == "" {
		return fmt.Errorf("vault Kubernetes login returned no client token")
	}
	t.client.SetToken(secret.Auth.ClientToken)
	t.authenticated = true
	if secret.Auth.LeaseDuration > 0 {
		t.tokenExpires = time.Now().Add(time.Duration(secret.Auth.LeaseDuration) * time.Second)
	} else {
		t.tokenExpires = time.Time{}
	}
	return nil
}
