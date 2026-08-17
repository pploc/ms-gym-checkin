package unit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pploc/ms-gym-checkin/internal/domain"
	"github.com/pploc/ms-gym-checkin/internal/usecase"
	"github.com/pploc/ms-gym-checkin/internal/usecase/port"
)

type readyStore struct{ err error }

func (s readyStore) FindIdempotency(context.Context, string, string) (*domain.IdempotencyResult, error) {
	return nil, nil
}

func (s readyStore) CreateCheckIn(context.Context, domain.CheckInRecord, string, string, domain.OutboxEvent) (domain.CheckInRecord, error) {
	return domain.CheckInRecord{}, nil
}

func (s readyStore) ListByUser(context.Context, string, int, int) ([]domain.CheckInRecord, int, error) {
	return nil, 0, nil
}

func (s readyStore) ListByMember(context.Context, string, int, int) ([]domain.CheckInRecord, int, error) {
	return nil, 0, nil
}

func (s readyStore) DailyCount(context.Context, string, time.Time, time.Time) (int, error) {
	return 0, nil
}
func (s readyStore) CurrentKey(context.Context, string) (*domain.RootKey, error) { return nil, nil }
func (s readyStore) FindKey(context.Context, string, uint64) (*domain.RootKey, error) {
	return nil, nil
}
func (s readyStore) CreateCurrentKey(context.Context, domain.RootKey) (domain.RootKey, error) {
	return domain.RootKey{}, nil
}
func (s readyStore) RotateKey(context.Context, domain.RootKey, time.Time, bool) (domain.RootKey, error) {
	return domain.RootKey{}, nil
}
func (s readyStore) ClaimOutbox(context.Context, int, time.Time) ([]domain.OutboxEvent, error) {
	return nil, nil
}
func (s readyStore) MarkPublished(context.Context, string, time.Time) error { return nil }
func (s readyStore) MarkRetry(context.Context, string, time.Time) error     { return nil }
func (s readyStore) Ping(context.Context) error                             { return s.err }
func (s readyStore) Close() error                                           { return nil }

type readyMember struct{ err error }

func (m readyMember) ValidateMembership(context.Context, string, string) (port.Membership, error) {
	return port.Membership{}, nil
}
func (m readyMember) Ping(context.Context) error { return m.err }
func (m readyMember) Close() error               { return nil }

type readyPlans struct{ err error }

func (p readyPlans) ValidateCheckInGym(context.Context, string) (port.Gym, error) {
	return port.Gym{}, nil
}
func (p readyPlans) Ping(context.Context) error { return p.err }
func (p readyPlans) Close() error               { return nil }

type readyProtector struct{ err error }

func (p readyProtector) KeyReference() string                            { return "key" }
func (p readyProtector) Encrypt(context.Context, []byte) (string, error) { return "", nil }
func (p readyProtector) Decrypt(context.Context, string, string) ([]byte, error) {
	return nil, nil
}
func (p readyProtector) Ping(context.Context) error { return p.err }

func TestGivenUnavailablePlans_WhenCheckingReadiness_ThenReturnsDependencyFailure(t *testing.T) {
	// Given
	dependencyErr := errors.New("plans unavailable")
	service := usecase.NewService(readyStore{}, readyMember{}, readyPlans{err: dependencyErr}, readyProtector{}, fixedClock{}, &ids{})

	// When
	err := service.Ready(context.Background())

	// Then
	if !errors.Is(err, dependencyErr) {
		t.Fatal("expected Plans readiness failure")
	}
}
