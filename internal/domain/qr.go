package domain

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const QRSlotDuration = time.Minute

type SignedQR struct {
	GymID      string
	KeyVersion uint64
	Slot       int64
	MAC        []byte
}

func CanonicalGymID(value string) (string, error) {
	parsed, err := uuid.Parse(value)
	if err != nil || parsed.String() != value {
		return "", fmt.Errorf("invalid gym id")
	}
	return value, nil
}

func ParseSignedQR(value string) (SignedQR, error) {
	parts := strings.Split(value, ".")
	if len(parts) != 5 || parts[0] != "v1" {
		return SignedQR{}, fmt.Errorf("invalid QR payload")
	}
	gymID, err := CanonicalGymID(parts[1])
	if err != nil {
		return SignedQR{}, err
	}
	keyVersion, err := parsePositiveUint(parts[2])
	if err != nil {
		return SignedQR{}, fmt.Errorf("invalid key version")
	}
	slot, err := parsePositiveInt(parts[3])
	if err != nil {
		return SignedQR{}, fmt.Errorf("invalid QR slot")
	}
	mac, err := base64.RawURLEncoding.DecodeString(parts[4])
	if err != nil || len(mac) != sha256.Size || base64.RawURLEncoding.EncodeToString(mac) != parts[4] {
		return SignedQR{}, fmt.Errorf("invalid QR signature")
	}
	return SignedQR{GymID: gymID, KeyVersion: keyVersion, Slot: slot, MAC: mac}, nil
}

func SignQR(gymID string, keyVersion uint64, at time.Time, key []byte) (string, error) {
	gymID, err := CanonicalGymID(gymID)
	if err != nil {
		return "", err
	}
	if keyVersion == 0 || len(key) == 0 {
		return "", fmt.Errorf("invalid signing key")
	}
	slot := at.UTC().Unix() / int64(QRSlotDuration/time.Second)
	if slot <= 0 {
		return "", fmt.Errorf("invalid QR slot")
	}
	message := qrMessage(gymID, keyVersion, slot)
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(message))
	return "v1." + gymID + "." + strconv.FormatUint(keyVersion, 10) + "." + strconv.FormatInt(slot, 10) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func VerifyQR(parsed SignedQR, key []byte, now time.Time) bool {
	current := now.UTC().Unix() / int64(QRSlotDuration/time.Second)
	if parsed.Slot != current && parsed.Slot != current-1 {
		return false
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(qrMessage(parsed.GymID, parsed.KeyVersion, parsed.Slot)))
	return hmac.Equal(parsed.MAC, mac.Sum(nil))
}

func SlotStart(slot int64) time.Time {
	return time.Unix(slot*int64(QRSlotDuration/time.Second), 0).UTC()
}

func qrMessage(gymID string, keyVersion uint64, slot int64) string {
	return "checkin-qr:v1|" + gymID + "|" + strconv.FormatUint(keyVersion, 10) + "|" + strconv.FormatInt(slot, 10)
}

func parsePositiveUint(value string) (uint64, error) {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return 0, fmt.Errorf("invalid integer")
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || parsed == 0 {
		return 0, fmt.Errorf("invalid integer")
	}
	return parsed, nil
}

func parsePositiveInt(value string) (int64, error) {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return 0, fmt.Errorf("invalid integer")
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("invalid integer")
	}
	return parsed, nil
}
