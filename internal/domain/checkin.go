package domain

import "time"

type CheckInRecord struct {
	ID          string
	UserID      string
	MemberID    string
	GymID       string
	CheckedInAt time.Time
}

type IdempotencyResult struct {
	Fingerprint string
	Record      CheckInRecord
}

type RootKeyStatus string

const (
	RootKeyCurrent  RootKeyStatus = "CURRENT"
	RootKeyPrevious RootKeyStatus = "PREVIOUS"
	RootKeyRetired  RootKeyStatus = "RETIRED"
)

type RootKey struct {
	GymID              string
	Version            uint64
	Ciphertext         string
	KeyReference       string
	Status             RootKeyStatus
	ActivatedAt        time.Time
	AcceptanceDeadline time.Time
	RetiredAt          *time.Time
}

func (k RootKey) IsAcceptableAt(now time.Time) bool {
	if k.Status == RootKeyCurrent {
		return true
	}
	return k.Status == RootKeyPrevious && now.Before(k.AcceptanceDeadline)
}

type OutboxEvent struct {
	ID        string
	Topic     string
	Key       string
	Payload   []byte
	Attempts  int
	CreatedAt time.Time
}
