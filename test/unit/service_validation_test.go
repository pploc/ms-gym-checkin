package unit

import (
	"context"
	"errors"
	"testing"
	"time"

	commonerrors "github.com/pploc/common-go/errors"

	"github.com/pploc/ms-gym-checkin/internal/domain"
	"github.com/pploc/ms-gym-checkin/internal/usecase"
)

func TestGivenReusedIdempotencyKeyWithDifferentPayload_WhenScanning_ThenReturnsConflict(t *testing.T) {
	// Given
	now := time.Date(2026, 8, 17, 10, 0, 30, 0, time.UTC)
	key := []byte("01234567890123456789012345678901")
	payload, err := domain.SignQR(gymID, 1, now, key)
	if err != nil {
		t.Fatal("sign idempotency test QR failed")
	}
	store := &memoryStore{key: &domain.RootKey{GymID: gymID, Version: 1, Ciphertext: string(key), Status: domain.RootKeyCurrent}}
	service := usecase.NewService(store, member{}, plans{}, protector{}, fixedClock{now}, &ids{})
	if _, err := service.Scan(context.Background(), "user", gymID, payload, "idempotency"); err != nil {
		t.Fatal("create initial idempotency check-in failed")
	}

	// When
	_, err = service.Scan(context.Background(), "user", gymID, payload+"x", "idempotency")

	// Then
	assertCode(t, err, "IDEMPOTENCY_CONFLICT")
}

func TestGivenInvalidDate_WhenCountingDailyCheckIns_ThenReturnsValidationError(t *testing.T) {
	// Given
	service := usecase.NewService(&memoryStore{}, member{}, plans{}, protector{}, fixedClock{}, &ids{})

	// When
	_, err := service.DailyCount(context.Background(), gymID, "2026-99-99")

	// Then
	assertCode(t, err, "INVALID_DATE")
}

func TestGivenNegativePage_WhenListingHistory_ThenReturnsValidationError(t *testing.T) {
	// Given
	service := usecase.NewService(&memoryStore{}, member{}, plans{}, protector{}, fixedClock{}, &ids{})

	// When
	_, _, err := service.MyHistory(context.Background(), "user", -1, 50)

	// Then
	assertCode(t, err, "INVALID_PAGE")
}

func TestGivenPreviousKeyWithinOverlap_WhenScanning_ThenAcceptsQR(t *testing.T) {
	// Given
	now := time.Date(2026, 8, 17, 10, 0, 30, 0, time.UTC)
	key := []byte("01234567890123456789012345678901")
	payload, err := domain.SignQR(gymID, 1, now, key)
	if err != nil {
		t.Fatal("sign previous-key test QR failed")
	}
	store := &memoryStore{key: &domain.RootKey{GymID: gymID, Version: 1, Ciphertext: string(key), Status: domain.RootKeyPrevious, AcceptanceDeadline: now.Add(time.Second)}}
	service := usecase.NewService(store, member{}, plans{}, protector{}, fixedClock{now}, &ids{})

	// When
	_, err = service.Scan(context.Background(), "user", gymID, payload, "idempotency")

	// Then
	if err != nil {
		t.Fatal("scan previous-key test QR failed")
	}
}

func TestGivenRetiredKey_WhenScanning_ThenRejectsQR(t *testing.T) {
	// Given
	now := time.Date(2026, 8, 17, 10, 0, 30, 0, time.UTC)
	key := []byte("01234567890123456789012345678901")
	payload, err := domain.SignQR(gymID, 1, now, key)
	if err != nil {
		t.Fatal("sign retired-key test QR failed")
	}
	store := &memoryStore{key: &domain.RootKey{GymID: gymID, Version: 1, Ciphertext: string(key), Status: domain.RootKeyRetired}}
	service := usecase.NewService(store, member{}, plans{}, protector{}, fixedClock{now}, &ids{})

	// When
	_, err = service.Scan(context.Background(), "user", gymID, payload, "idempotency")

	// Then
	assertCode(t, err, "INVALID_QR")
}

func assertCode(t *testing.T, err error, want string) {
	t.Helper()
	var categorized *commonerrors.Error
	if !errors.As(err, &categorized) || categorized.Code != want {
		t.Fatal("unexpected error code")
	}
}
