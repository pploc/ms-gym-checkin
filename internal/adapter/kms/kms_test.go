package kms

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
)

type fakeClient struct {
	describe func(*awskms.DescribeKeyInput) (*awskms.DescribeKeyOutput, error)
	encrypt  func(*awskms.EncryptInput) (*awskms.EncryptOutput, error)
	decrypt  func(*awskms.DecryptInput) (*awskms.DecryptOutput, error)
}

func (f fakeClient) DescribeKey(_ context.Context, input *awskms.DescribeKeyInput, _ ...func(*awskms.Options)) (*awskms.DescribeKeyOutput, error) {
	return f.describe(input)
}

func (f fakeClient) Encrypt(_ context.Context, input *awskms.EncryptInput, _ ...func(*awskms.Options)) (*awskms.EncryptOutput, error) {
	return f.encrypt(input)
}

func (f fakeClient) Decrypt(_ context.Context, input *awskms.DecryptInput, _ ...func(*awskms.Options)) (*awskms.DecryptOutput, error) {
	return f.decrypt(input)
}

func TestGivenKMSKey_WhenCreatingProtectorAndRoundTripping_ThenUsesCanonicalARNAndBase64Ciphertext(t *testing.T) {
	// Given
	plaintext := []byte("01234567890123456789012345678901")
	ciphertext := []byte{1, 2, 3}
	const keyARN = "arn:aws:kms:us-east-1:123456789012:key/checkin-root"
	client := fakeClient{
		describe: func(input *awskms.DescribeKeyInput) (*awskms.DescribeKeyOutput, error) {
			if aws.ToString(input.KeyId) != "alias/checkin-root" {
				t.Fatal("unexpected KMS key ID")
			}
			return enabledSymmetricKey(keyARN), nil
		},
		encrypt: func(input *awskms.EncryptInput) (*awskms.EncryptOutput, error) {
			if string(input.Plaintext) != string(plaintext) || input.EncryptionAlgorithm != types.EncryptionAlgorithmSpecSymmetricDefault || aws.ToString(input.KeyId) != keyARN {
				t.Fatal("unexpected KMS encrypt input")
			}
			return &awskms.EncryptOutput{CiphertextBlob: ciphertext}, nil
		},
		decrypt: func(input *awskms.DecryptInput) (*awskms.DecryptOutput, error) {
			if string(input.CiphertextBlob) != string(ciphertext) || aws.ToString(input.KeyId) != keyARN {
				t.Fatal("unexpected KMS decrypt input")
			}
			return &awskms.DecryptOutput{Plaintext: plaintext}, nil
		},
	}
	protector, err := newProtector(context.Background(), client, "alias/checkin-root")
	if err != nil {
		t.Fatal("create KMS protector failed")
	}

	// When
	encoded, err := protector.Encrypt(context.Background(), plaintext)
	if err != nil {
		t.Fatal("encrypt root key failed")
	}
	decrypted, err := protector.Decrypt(context.Background(), protector.KeyReference(), encoded)

	// Then
	if protector.KeyReference() != keyARN {
		t.Fatal("canonical KMS ARN was not retained")
	}
	if encoded != base64.StdEncoding.EncodeToString(ciphertext) || encoded == string(plaintext) {
		t.Fatal("ciphertext was not encoded as opaque base64")
	}
	if err != nil || string(decrypted) != string(plaintext) {
		t.Fatal("KMS round trip failed")
	}
}

func TestGivenDisabledOrIncompatibleKMSKey_WhenCreatingProtector_ThenRejectsStartup(t *testing.T) {
	// Given
	metadata := enabledSymmetricKey("arn")
	metadata.KeyMetadata.KeyState = types.KeyStateDisabled
	client := fakeClient{
		describe: func(*awskms.DescribeKeyInput) (*awskms.DescribeKeyOutput, error) { return metadata, nil },
	}

	// When
	_, err := newProtector(context.Background(), client, "key")

	// Then
	if err == nil {
		t.Fatal("expected disabled KMS key rejection")
	}
}

func TestGivenMissingKMSResponseData_WhenUsingProtector_ThenReturnsError(t *testing.T) {
	// Given
	client := fakeClient{
		describe: func(*awskms.DescribeKeyInput) (*awskms.DescribeKeyOutput, error) {
			return enabledSymmetricKey("arn"), nil
		},
		encrypt: func(*awskms.EncryptInput) (*awskms.EncryptOutput, error) { return nil, nil },
		decrypt: func(*awskms.DecryptInput) (*awskms.DecryptOutput, error) { return nil, nil },
	}
	protector, err := newProtector(context.Background(), client, "key")
	if err != nil {
		t.Fatal("create KMS protector failed")
	}

	// When
	_, encryptErr := protector.Encrypt(context.Background(), []byte("key"))
	_, decryptErr := protector.Decrypt(context.Background(), protector.KeyReference(), base64.StdEncoding.EncodeToString([]byte("ciphertext")))

	// Then
	if encryptErr == nil || decryptErr == nil {
		t.Fatal("expected missing KMS response error")
	}
}

func TestGivenPersistedDifferentKMSReference_WhenDecrypting_ThenFailsBeforeCallingKMS(t *testing.T) {
	// Given
	called := false
	client := fakeClient{
		describe: func(*awskms.DescribeKeyInput) (*awskms.DescribeKeyOutput, error) {
			return enabledSymmetricKey("arn:aws:kms:us-east-1:123456789012:key/configured"), nil
		},
		decrypt: func(*awskms.DecryptInput) (*awskms.DecryptOutput, error) {
			called = true
			return &awskms.DecryptOutput{Plaintext: []byte("key")}, nil
		},
	}
	protector, err := newProtector(context.Background(), client, "alias/checkin-root")
	if err != nil {
		t.Fatal("create KMS protector failed")
	}

	// When
	_, err = protector.Decrypt(context.Background(), "arn:aws:kms:us-east-1:999999999999:key/other", base64.StdEncoding.EncodeToString([]byte("ciphertext")))

	// Then
	if err == nil || called {
		t.Fatal("persisted mismatched KMS reference was not rejected before decrypt")
	}
}

func TestGivenMalformedCiphertextOrKMSFailure_WhenDecrypting_ThenReturnsError(t *testing.T) {
	// Given
	providerErr := errors.New("KMS unavailable")
	client := fakeClient{
		describe: func(*awskms.DescribeKeyInput) (*awskms.DescribeKeyOutput, error) {
			return enabledSymmetricKey("arn"), nil
		},
		encrypt: func(*awskms.EncryptInput) (*awskms.EncryptOutput, error) { return nil, providerErr },
		decrypt: func(*awskms.DecryptInput) (*awskms.DecryptOutput, error) { return nil, providerErr },
	}
	protector, err := newProtector(context.Background(), client, "key")
	if err != nil {
		t.Fatal("create KMS protector failed")
	}

	// When
	_, malformedErr := protector.Decrypt(context.Background(), protector.KeyReference(), "not base64")
	_, encryptErr := protector.Encrypt(context.Background(), []byte("key"))
	_, decryptErr := protector.Decrypt(context.Background(), protector.KeyReference(), base64.StdEncoding.EncodeToString([]byte("ciphertext")))

	// Then
	if malformedErr == nil || !errors.Is(encryptErr, providerErr) || !errors.Is(decryptErr, providerErr) {
		t.Fatal("expected KMS failures")
	}
}

func TestGivenNonLocalKMSEndpoint_WhenCreatingProtector_ThenFailsClosed(t *testing.T) {
	// Given
	endpoint := "http://kms.example.test"

	// When
	_, err := New(context.Background(), "us-east-1", "alias/checkin-root", endpoint)

	// Then
	if err == nil {
		t.Fatal("expected non-local KMS endpoint rejection")
	}
}

func TestGivenKMSLocalEndpoint_WhenValidating_ThenOnlyAllowsLocalStack(t *testing.T) {
	// Given
	endpoints := map[string]bool{
		"http://127.0.0.1:4566":               true,
		"http://localstack:4566":              true,
		"https://kms.us-east-1.amazonaws.com": false,
		"http://kms.example.test":             false,
	}

	// When and Then
	for endpoint, expected := range endpoints {
		if isLocalEndpoint(endpoint) != expected {
			t.Fatalf("unexpected local endpoint validation for %s", endpoint)
		}
	}
}

func enabledSymmetricKey(arn string) *awskms.DescribeKeyOutput {
	return &awskms.DescribeKeyOutput{KeyMetadata: &types.KeyMetadata{
		Arn:      aws.String(arn),
		KeyState: types.KeyStateEnabled,
		KeyUsage: types.KeyUsageTypeEncryptDecrypt,
		KeySpec:  types.KeySpecSymmetricDefault,
	}}
}
