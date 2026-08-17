package unit

import (
	"strings"
	"testing"
	"time"

	"github.com/pploc/ms-gym-checkin/internal/domain"
)

const gymID = "11111111-1111-1111-1111-111111111111"

func TestGivenCanonicalQR_WhenParsingAndVerifying_ThenAcceptsCurrentSlot(t *testing.T) {
	// Given
	key := []byte("01234567890123456789012345678901")
	now := time.Date(2026, 8, 17, 10, 0, 30, 0, time.UTC)
	payload, err := domain.SignQR(gymID, 1, now, key)
	if err != nil {
		t.Fatal(err)
	}

	// When
	parsed, err := domain.ParseSignedQR(payload)

	// Then
	if err != nil || !domain.VerifyQR(parsed, key, now) {
		t.Fatalf("expected valid QR: %v", err)
	}
}

func TestGivenUppercaseGymID_WhenSigning_ThenRejectsNonCanonicalIdentity(t *testing.T) {
	// Given
	upper := strings.ToUpper("a0b1c2d3-e4f5-4a6b-8c9d-0e1f2a3b4c5d")

	// When
	_, err := domain.SignQR(upper, 1, time.Now(), []byte("01234567890123456789012345678901"))

	// Then
	if err == nil {
		t.Fatal("expected noncanonical gym rejection")
	}
}

func TestGivenSignedQRFromOldSlot_WhenVerifying_ThenRejects(t *testing.T) {
	// Given
	key := []byte("01234567890123456789012345678901")
	now := time.Date(2026, 8, 17, 10, 5, 0, 0, time.UTC)
	payload, err := domain.SignQR(gymID, 1, now.Add(-2*time.Minute), key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := domain.ParseSignedQR(payload)
	if err != nil {
		t.Fatal(err)
	}

	// When
	valid := domain.VerifyQR(parsed, key, now)

	// Then
	if valid {
		t.Fatal("expected expired slot rejection")
	}
}
