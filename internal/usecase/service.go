package usecase

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	commonerrors "github.com/pploc/common-go/errors"
	eventsv1 "github.com/pploc/proto-go/events/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/pploc/ms-gym-checkin/internal/domain"
	"github.com/pploc/ms-gym-checkin/internal/usecase/port"
)

const (
	checkInTopic    = "checkin.recorded.v1"
	serviceName     = "ms-gym-checkin"
	priorKeyOverlap = 120 * time.Second
)

type Service struct {
	store     port.Store
	member    port.MemberClient
	plans     port.PlansClient
	protector port.KeyProtector
	clock     port.Clock
	ids       port.IDGenerator
	keys      *keyCache
}

func NewService(store port.Store, member port.MemberClient, plans port.PlansClient, protector port.KeyProtector, clock port.Clock, ids port.IDGenerator) *Service {
	return &Service{store: store, member: member, plans: plans, protector: protector, clock: clock, ids: ids, keys: newKeyCache(128)}
}

type DisplayPayload struct {
	GymID               string
	Current             string
	CurrentActiveAt     time.Time
	CurrentExpiresAt    time.Time
	Next                string
	NextActiveAt        time.Time
	NextExpiresAt       time.Time
	SlotDurationSeconds int32
}

type Rotation struct {
	GymID       string
	KeyVersion  uint64
	ActivatedAt time.Time
}

func (s *Service) Display(ctx context.Context, gymID string) (DisplayPayload, error) {
	gym, err := s.plans.ValidateCheckInGym(ctx, gymID)
	if err != nil {
		return DisplayPayload{}, err
	}
	key, err := s.currentKey(ctx, gym.ID)
	if err != nil {
		return DisplayPayload{}, err
	}
	plain, err := s.key(ctx, key)
	if err != nil {
		return DisplayPayload{}, err
	}
	defer clear(plain)
	now := s.clock.Now().UTC()
	slot := now.Unix() / int64(domain.QRSlotDuration/time.Second)
	currentAt := domain.SlotStart(slot)
	nextAt := currentAt.Add(domain.QRSlotDuration)
	current, err := domain.SignQR(gym.ID, key.Version, currentAt, plain)
	if err != nil {
		return DisplayPayload{}, err
	}
	next, err := domain.SignQR(gym.ID, key.Version, nextAt, plain)
	if err != nil {
		return DisplayPayload{}, err
	}
	return DisplayPayload{gym.ID, current, currentAt, nextAt, next, nextAt, nextAt.Add(domain.QRSlotDuration), int32(domain.QRSlotDuration / time.Second)}, nil
}

func (s *Service) Rotate(ctx context.Context, gymID string, emergency bool) (Rotation, error) {
	gym, err := s.plans.ValidateCheckInGym(ctx, gymID)
	if err != nil {
		return Rotation{}, err
	}
	previous, err := s.store.CurrentKey(ctx, gym.ID)
	if err != nil {
		return Rotation{}, err
	}
	if previous == nil {
		key, err := s.currentKey(ctx, gym.ID)
		if err != nil {
			return Rotation{}, err
		}
		return Rotation{key.GymID, key.Version, key.ActivatedAt}, nil
	}
	key, err := s.newKey(ctx, gym.ID, previous.Version+1)
	if err != nil {
		return Rotation{}, err
	}
	now := s.clock.Now().UTC()
	deadline := now.Add(priorKeyOverlap)
	if emergency {
		deadline = now
	}
	created, err := s.store.RotateKey(ctx, key, deadline, emergency)
	if err != nil {
		return Rotation{}, err
	}
	s.keys.delete(previous.GymID, previous.Version)
	return Rotation{created.GymID, created.Version, created.ActivatedAt}, nil
}

func (s *Service) Scan(ctx context.Context, userID, gymID, payload, idempotencyKey string) (domain.CheckInRecord, error) {
	fingerprint := fingerprint(gymID, payload)
	existing, err := s.store.FindIdempotency(ctx, userID, idempotencyKey)
	if err != nil {
		return domain.CheckInRecord{}, err
	}
	if existing != nil {
		if existing.Fingerprint != fingerprint {
			return domain.CheckInRecord{}, commonerrors.New(commonerrors.CategoryConflict, "IDEMPOTENCY_CONFLICT", "idempotency key was already used")
		}
		return existing.Record, nil
	}
	parsed, err := domain.ParseSignedQR(payload)
	if err != nil || parsed.GymID != gymID {
		return domain.CheckInRecord{}, commonerrors.New(commonerrors.CategoryValidation, "INVALID_QR", "QR payload is invalid")
	}
	key, err := s.store.FindKey(ctx, parsed.GymID, parsed.KeyVersion)
	if err != nil || key == nil || !key.IsAcceptableAt(s.clock.Now().UTC()) {
		return domain.CheckInRecord{}, commonerrors.New(commonerrors.CategoryValidation, "INVALID_QR", "QR payload is invalid")
	}
	plain, err := s.key(ctx, *key)
	if err != nil {
		return domain.CheckInRecord{}, err
	}
	valid := domain.VerifyQR(parsed, plain, s.clock.Now())
	clear(plain)
	if !valid {
		return domain.CheckInRecord{}, commonerrors.New(commonerrors.CategoryValidation, "INVALID_QR", "QR payload is invalid")
	}
	membership, err := s.member.ValidateMembership(ctx, userID, parsed.GymID)
	if err != nil {
		return domain.CheckInRecord{}, err
	}
	if !membership.Valid || membership.Status != "ACTIVE" {
		return domain.CheckInRecord{}, commonerrors.New(commonerrors.CategoryUnprocessable, "MEMBERSHIP_INACTIVE", "membership is not active")
	}
	now := s.clock.Now().UTC()
	record := domain.CheckInRecord{ID: s.ids.New(), UserID: userID, MemberID: membership.MemberID, GymID: parsed.GymID, CheckedInAt: now}
	event, err := proto.Marshal(&eventsv1.CheckInRecordedEvent{MemberId: record.MemberID, GymId: record.GymID, CheckedInAt: timestamppb.New(record.CheckedInAt)})
	if err != nil {
		return domain.CheckInRecord{}, fmt.Errorf("marshal check-in event: %w", err)
	}
	outbox := domain.OutboxEvent{ID: s.ids.New(), Topic: checkInTopic, Key: record.MemberID, Payload: event, CreatedAt: now}
	return s.store.CreateCheckIn(ctx, record, fingerprint, idempotencyKey, outbox)
}

func (s *Service) MyHistory(ctx context.Context, userID string, page, limit int) ([]domain.CheckInRecord, int, error) {
	if page < 0 {
		return nil, 0, commonerrors.New(commonerrors.CategoryValidation, "INVALID_PAGE", "page is invalid")
	}
	return s.store.ListByUser(ctx, userID, page, paginationLimit(limit))
}

func (s *Service) MemberHistory(ctx context.Context, memberID string, page, limit int) ([]domain.CheckInRecord, int, error) {
	if page < 0 {
		return nil, 0, commonerrors.New(commonerrors.CategoryValidation, "INVALID_PAGE", "page is invalid")
	}
	return s.store.ListByMember(ctx, memberID, page, paginationLimit(limit))
}

func (s *Service) DailyCount(ctx context.Context, gymID, date string) (int, error) {
	day, err := time.Parse("2006-01-02", date)
	if err != nil {
		return 0, commonerrors.New(commonerrors.CategoryValidation, "INVALID_DATE", "date is invalid")
	}
	return s.store.DailyCount(ctx, gymID, day.UTC(), day.UTC().AddDate(0, 0, 1))
}

func (s *Service) Ready(ctx context.Context) error {
	if err := s.store.Ping(ctx); err != nil {
		return err
	}
	if err := s.protector.Ping(ctx); err != nil {
		return err
	}
	if err := s.member.Ping(ctx); err != nil {
		return err
	}
	return s.plans.Ping(ctx)
}

func (s *Service) currentKey(ctx context.Context, gymID string) (domain.RootKey, error) {
	key, err := s.store.CurrentKey(ctx, gymID)
	if err != nil {
		return domain.RootKey{}, err
	}
	if key != nil {
		return *key, nil
	}
	created, err := s.newKey(ctx, gymID, 1)
	if err != nil {
		return domain.RootKey{}, err
	}
	return s.store.CreateCurrentKey(ctx, created)
}

func (s *Service) newKey(ctx context.Context, gymID string, version uint64) (domain.RootKey, error) {
	plain := make([]byte, 32)
	if _, err := rand.Read(plain); err != nil {
		return domain.RootKey{}, fmt.Errorf("generate QR key: %w", err)
	}
	defer clear(plain)
	ciphertext, err := s.protector.Encrypt(ctx, plain)
	if err != nil {
		return domain.RootKey{}, err
	}
	now := s.clock.Now().UTC()
	return domain.RootKey{GymID: gymID, Version: version, Ciphertext: ciphertext, KeyReference: s.protector.KeyReference(), Status: domain.RootKeyCurrent, ActivatedAt: now}, nil
}

func (s *Service) key(ctx context.Context, key domain.RootKey) ([]byte, error) {
	// A key may have been rotated by another process. Never reuse a cache entry
	// created while it was CURRENT after Yugabyte now reports it as PREVIOUS.
	if key.Status != domain.RootKeyCurrent {
		s.keys.delete(key.GymID, key.Version)
	}
	if cached, ok := s.keys.get(key.GymID, key.Version, s.clock.Now()); ok {
		return cached, nil
	}
	plain, err := s.protector.Decrypt(ctx, key.KeyReference, key.Ciphertext)
	if err != nil {
		return nil, commonerrors.New(commonerrors.CategoryUnavailable, "VAULT_UNAVAILABLE", "key service is unavailable")
	}
	defer clear(plain)
	if len(plain) != 32 {
		return nil, commonerrors.New(commonerrors.CategoryUnavailable, "VAULT_UNAVAILABLE", "key service is unavailable")
	}
	expires := key.AcceptanceDeadline
	if key.Status == domain.RootKeyCurrent || expires.IsZero() {
		expires = s.clock.Now().UTC().Add(5 * time.Minute)
	}
	s.keys.put(key.GymID, key.Version, plain, expires)
	return append([]byte(nil), plain...), nil
}

func paginationLimit(limit int) int {
	if limit <= 0 {
		return 50
	}
	return limit
}
func fingerprint(gymID, payload string) string {
	sum := sha256.Sum256([]byte(gymID + "\x00" + payload))
	return hex.EncodeToString(sum[:])
}
func clear(value []byte) {
	for i := range value {
		value[i] = 0
	}
}
