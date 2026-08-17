package kms

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
)

type client interface {
	DescribeKey(context.Context, *awskms.DescribeKeyInput, ...func(*awskms.Options)) (*awskms.DescribeKeyOutput, error)
	Encrypt(context.Context, *awskms.EncryptInput, ...func(*awskms.Options)) (*awskms.EncryptOutput, error)
	Decrypt(context.Context, *awskms.DecryptInput, ...func(*awskms.Options)) (*awskms.DecryptOutput, error)
}

type Protector struct {
	client       client
	keyReference string
}

func New(ctx context.Context, region, keyID, endpointURL string) (*Protector, error) {
	if endpointURL != "" && !isLocalEndpoint(endpointURL) {
		return nil, fmt.Errorf("KMS_ENDPOINT_URL must target local LocalStack")
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}
	clientOptions := []func(*awskms.Options){}
	if endpointURL != "" {
		clientOptions = append(clientOptions, func(options *awskms.Options) {
			options.BaseEndpoint = aws.String(endpointURL)
		})
	}
	return newProtector(ctx, awskms.NewFromConfig(cfg, clientOptions...), keyID)
}

func newProtector(ctx context.Context, client client, keyID string) (*Protector, error) {
	keyReference, err := describeKey(ctx, client, keyID)
	if err != nil {
		return nil, err
	}
	return &Protector{client: client, keyReference: keyReference}, nil
}

func (p *Protector) KeyReference() string { return p.keyReference }

func (p *Protector) Encrypt(ctx context.Context, plaintext []byte) (string, error) {
	response, err := p.client.Encrypt(ctx, &awskms.EncryptInput{KeyId: &p.keyReference, Plaintext: plaintext, EncryptionAlgorithm: types.EncryptionAlgorithmSpecSymmetricDefault})
	if err != nil {
		return "", fmt.Errorf("encrypt with KMS: %w", err)
	}
	if response == nil || len(response.CiphertextBlob) == 0 {
		return "", fmt.Errorf("KMS returned no ciphertext")
	}
	return base64.StdEncoding.EncodeToString(response.CiphertextBlob), nil
}

func (p *Protector) Decrypt(ctx context.Context, keyReference, ciphertext string) ([]byte, error) {
	blob, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return nil, fmt.Errorf("decode KMS ciphertext: %w", err)
	}
	response, err := p.client.Decrypt(ctx, &awskms.DecryptInput{CiphertextBlob: blob, KeyId: &keyReference, EncryptionAlgorithm: types.EncryptionAlgorithmSpecSymmetricDefault})
	if err != nil {
		return nil, fmt.Errorf("decrypt with KMS: %w", err)
	}
	if response == nil || len(response.Plaintext) == 0 {
		return nil, fmt.Errorf("KMS returned no plaintext")
	}
	return response.Plaintext, nil
}

func (p *Protector) Ping(ctx context.Context) error {
	_, err := describeKey(ctx, p.client, p.keyReference)
	return err
}

func describeKey(ctx context.Context, client client, keyID string) (string, error) {
	response, err := client.DescribeKey(ctx, &awskms.DescribeKeyInput{KeyId: &keyID})
	if err != nil {
		return "", fmt.Errorf("describe KMS key: %w", err)
	}
	if response == nil || response.KeyMetadata == nil || response.KeyMetadata.Arn == nil || *response.KeyMetadata.Arn == "" {
		return "", fmt.Errorf("KMS returned no key ARN")
	}
	metadata := response.KeyMetadata
	if metadata.KeyState != types.KeyStateEnabled || metadata.KeyUsage != types.KeyUsageTypeEncryptDecrypt || metadata.KeySpec != types.KeySpecSymmetricDefault {
		return "", fmt.Errorf("KMS key must be enabled symmetric ENCRYPT_DECRYPT")
	}
	return *metadata.Arn, nil
}

func isLocalEndpoint(value string) bool {
	parsed, err := url.ParseRequestURI(value)
	if err != nil || !strings.EqualFold(parsed.Scheme, "http") || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return false
	}
	switch strings.ToLower(parsed.Hostname()) {
	case "localhost", "127.0.0.1", "::1", "localstack":
		return true
	default:
		return false
	}
}
