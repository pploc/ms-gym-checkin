package vault

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"

	vaultapi "github.com/hashicorp/vault/api"
)

type Transit struct {
	client       *vaultapi.Client
	mount        string
	keyReference string
}

func New(address, token, mount, keyReference string) (*Transit, error) {
	cfg := vaultapi.DefaultConfig()
	cfg.Address = address
	client, err := vaultapi.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	client.SetToken(token)
	return &Transit{client: client, mount: strings.Trim(mount, "/"), keyReference: keyReference}, nil
}
func (t *Transit) KeyReference() string { return t.keyReference }

func (t *Transit) Encrypt(ctx context.Context, plaintext []byte) (string, error) {
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
	_, err := t.client.Sys().HealthWithContext(ctx)
	return err
}
