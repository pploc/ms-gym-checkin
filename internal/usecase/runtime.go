package usecase

import (
	"time"

	"github.com/google/uuid"
)

type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

type UUIDGenerator struct{}

func (UUIDGenerator) New() string { return uuid.NewString() }
