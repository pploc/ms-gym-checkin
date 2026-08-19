package unit

import (
	"context"
	"testing"
	"time"

	"github.com/pploc/ms-gym-checkin/internal/domain"
	"github.com/pploc/ms-gym-checkin/internal/usecase"
	"github.com/pploc/ms-gym-checkin/internal/usecase/port"
)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

type ids struct{ next int }

func (i *ids) New() string { i.next++; return string(rune('a' + i.next)) }

type memoryStore struct {
	idempotency *domain.IdempotencyResult
	key         *domain.RootKey
	record      domain.CheckInRecord
}

func (s *memoryStore) FindIdempotency(context.Context, string, string) (*domain.IdempotencyResult, error) {
	return s.idempotency, nil
}
func (s *memoryStore) CreateCheckIn(_ context.Context, record domain.CheckInRecord, fingerprint, key string, _ domain.OutboxEvent) (domain.CheckInRecord, error) {
	s.idempotency = &domain.IdempotencyResult{Fingerprint: fingerprint, Record: record}
	s.record = record
	return record, nil
}
func (s *memoryStore) ListByUser(context.Context, string, int, int) ([]domain.CheckInRecord, int, error) {
	return nil, 0, nil
}
func (s *memoryStore) ListByMember(context.Context, string, int, int) ([]domain.CheckInRecord, int, error) {
	return nil, 0, nil
}
func (s *memoryStore) DailyCount(context.Context, string, time.Time, time.Time) (int, error) {
	return 0, nil
}
func (s *memoryStore) CurrentKey(context.Context, string) (*domain.RootKey, error) { return s.key, nil }
func (s *memoryStore) FindKey(context.Context, string, uint64) (*domain.RootKey, error) {
	return s.key, nil
}
func (s *memoryStore) CreateCurrentKey(_ context.Context, key domain.RootKey) (domain.RootKey, error) {
	s.key = &key
	return key, nil
}
func (s *memoryStore) RotateKey(_ context.Context, key domain.RootKey, _ time.Time, _ bool) (domain.RootKey, error) {
	s.key = &key
	return key, nil
}
func (s *memoryStore) ClaimOutbox(context.Context, int, time.Time) ([]domain.OutboxEvent, error) {
	return nil, nil
}
func (s *memoryStore) SavePreparedOutbox(context.Context, string, []byte, []byte) error {
	return nil
}
func (s *memoryStore) MarkPublished(context.Context, string, time.Time) error  { return nil }
func (s *memoryStore) MarkRetry(context.Context, string, int, time.Time) error { return nil }
func (s *memoryStore) MarkFailed(context.Context, string, int) error           { return nil }
func (s *memoryStore) MarkInvalid(context.Context, string, int) error          { return nil }
func (s *memoryStore) Ping(context.Context) error                              { return nil }
func (s *memoryStore) Close() error                                            { return nil }

type member struct{}

func (member) ValidateMembership(context.Context, string, string) (port.Membership, error) {
	return port.Membership{MemberID: "member", Valid: true, Status: "ACTIVE"}, nil
}
func (member) Ping(context.Context) error { return nil }
func (member) Close() error               { return nil }

type plans struct{}

func (plans) ValidateCheckInGym(_ context.Context, id string) (port.Gym, error) {
	return port.Gym{ID: id, Status: "ACTIVE"}, nil
}
func (plans) Ping(context.Context) error { return nil }
func (plans) Close() error               { return nil }

type protector struct{}

func (protector) KeyReference() string                                    { return "kms-key" }
func (protector) Encrypt(_ context.Context, value []byte) (string, error) { return string(value), nil }
func (protector) Decrypt(_ context.Context, _ string, value string) ([]byte, error) {
	return []byte(value), nil
}
func (protector) Ping(context.Context) error { return nil }

type recordingProtector struct{ keyReference string }

func (p *recordingProtector) KeyReference() string { return "configured-kms-key" }
func (p *recordingProtector) Encrypt(_ context.Context, value []byte) (string, error) {
	return string(value), nil
}
func (p *recordingProtector) Decrypt(_ context.Context, keyReference, ciphertext string) ([]byte, error) {
	p.keyReference = keyReference
	return []byte(ciphertext), nil
}
func (p *recordingProtector) Ping(context.Context) error { return nil }

func TestGivenPersistedKMSKeyReference_WhenScanning_ThenDecryptsWithPersistedReference(t *testing.T) {
	// Given
	now := time.Date(2026, 8, 17, 10, 0, 30, 0, time.UTC)
	key := []byte("01234567890123456789012345678901")
	payload, err := domain.SignQR(gymID, 1, now, key)
	if err != nil {
		t.Fatal("sign QR failed")
	}
	protector := &recordingProtector{}
	store := &memoryStore{key: &domain.RootKey{GymID: gymID, Version: 1, Ciphertext: string(key), KeyReference: "arn:aws:kms:us-east-1:123456789012:key/retired-alias", Status: domain.RootKeyCurrent}}
	service := usecase.NewService(store, member{}, plans{}, protector, fixedClock{now}, &ids{})

	// When
	_, err = service.Scan(context.Background(), "user", gymID, payload, "idempotency")

	// Then
	if err != nil || protector.keyReference != store.key.KeyReference {
		t.Fatal("scan did not decrypt with persisted KMS key reference")
	}
}

func TestGivenExactReplay_WhenScanning_ThenReturnsStoredRecord(t *testing.T) {
	// Given
	now := time.Date(2026, 8, 17, 10, 0, 30, 0, time.UTC)
	store := &memoryStore{key: &domain.RootKey{GymID: gymID, Version: 1, Ciphertext: "01234567890123456789012345678901", Status: domain.RootKeyCurrent}}
	service := usecase.NewService(store, member{}, plans{}, protector{}, fixedClock{now}, &ids{})
	payload, err := domain.SignQR(gymID, 1, now, []byte("01234567890123456789012345678901"))
	if err != nil {
		t.Fatal("sign test QR failed")
	}

	// When
	first, err := service.Scan(context.Background(), "user", gymID, payload, "idempotency")
	if err != nil {
		t.Fatal("scan test QR failed")
	}
	second, err := service.Scan(context.Background(), "user", gymID, payload, "idempotency")

	// Then
	if err != nil || first.ID != second.ID {
		t.Fatal("expected stored replay")
	}
}
