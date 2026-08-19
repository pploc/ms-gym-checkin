package port

import (
	"context"
	"time"

	"github.com/pploc/ms-gym-checkin/internal/domain"
)

type Store interface {
	FindIdempotency(context.Context, string, string) (*domain.IdempotencyResult, error)
	CreateCheckIn(context.Context, domain.CheckInRecord, string, string, domain.OutboxEvent) (domain.CheckInRecord, error)
	ListByUser(context.Context, string, int, int) ([]domain.CheckInRecord, int, error)
	ListByMember(context.Context, string, int, int) ([]domain.CheckInRecord, int, error)
	DailyCount(context.Context, string, time.Time, time.Time) (int, error)
	CurrentKey(context.Context, string) (*domain.RootKey, error)
	FindKey(context.Context, string, uint64) (*domain.RootKey, error)
	CreateCurrentKey(context.Context, domain.RootKey) (domain.RootKey, error)
	RotateKey(context.Context, domain.RootKey, time.Time, bool) (domain.RootKey, error)
	ClaimOutbox(context.Context, int, time.Time) ([]domain.OutboxEvent, error)
	SavePreparedOutbox(context.Context, string, []byte, []byte) error
	MarkPublished(context.Context, string, time.Time) error
	MarkRetry(context.Context, string, int, time.Time) error
	MarkFailed(context.Context, string, int) error
	MarkInvalid(context.Context, string, int) error
	Ping(context.Context) error
	Close() error
}

type Membership struct {
	MemberID string
	Valid    bool
	Status   string
}

type MemberClient interface {
	ValidateMembership(context.Context, string, string) (Membership, error)
	Ping(context.Context) error
	Close() error
}

type Gym struct {
	ID     string
	Status string
}

type PlansClient interface {
	ValidateCheckInGym(context.Context, string) (Gym, error)
	Ping(context.Context) error
	Close() error
}

type KeyProtector interface {
	KeyReference() string
	Encrypt(context.Context, []byte) (string, error)
	Decrypt(context.Context, string, string) ([]byte, error)
	Ping(context.Context) error
}

type Clock interface {
	Now() time.Time
}

type IDGenerator interface {
	New() string
}
