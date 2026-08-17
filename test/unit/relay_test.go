package unit

import (
	"context"
	"errors"
	"testing"
	"time"

	commonkafka "github.com/pploc/common-go/kafka"
	eventsv1 "github.com/pploc/proto-go/events/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	kafkaadapter "github.com/pploc/ms-gym-checkin/internal/adapter/kafka"
	"github.com/pploc/ms-gym-checkin/internal/domain"
)

type relayStore struct {
	events    []domain.OutboxEvent
	published []string
	retried   []string
}

func (s *relayStore) ClaimOutbox(context.Context, int, time.Time) ([]domain.OutboxEvent, error) {
	return s.events, nil
}
func (s *relayStore) MarkPublished(_ context.Context, id string, _ time.Time) error {
	s.published = append(s.published, id)
	return nil
}
func (s *relayStore) MarkRetry(_ context.Context, id string, _ time.Time) error {
	s.retried = append(s.retried, id)
	return nil
}
func (s *relayStore) FindIdempotency(context.Context, string, string) (*domain.IdempotencyResult, error) {
	return nil, nil
}
func (s *relayStore) CreateCheckIn(context.Context, domain.CheckInRecord, string, string, domain.OutboxEvent) (domain.CheckInRecord, error) {
	return domain.CheckInRecord{}, nil
}
func (s *relayStore) ListByUser(context.Context, string, int, int) ([]domain.CheckInRecord, int, error) {
	return nil, 0, nil
}
func (s *relayStore) ListByMember(context.Context, string, int, int) ([]domain.CheckInRecord, int, error) {
	return nil, 0, nil
}
func (s *relayStore) DailyCount(context.Context, string, time.Time, time.Time) (int, error) {
	return 0, nil
}
func (s *relayStore) CurrentKey(context.Context, string) (*domain.RootKey, error) { return nil, nil }
func (s *relayStore) FindKey(context.Context, string, uint64) (*domain.RootKey, error) {
	return nil, nil
}
func (s *relayStore) CreateCurrentKey(context.Context, domain.RootKey) (domain.RootKey, error) {
	return domain.RootKey{}, nil
}
func (s *relayStore) RotateKey(context.Context, domain.RootKey, time.Time, bool) (domain.RootKey, error) {
	return domain.RootKey{}, nil
}
func (s *relayStore) Ping(context.Context) error { return nil }
func (s *relayStore) Close() error               { return nil }

type relayProducer struct {
	events []commonkafka.Event
	err    error
}

func (p *relayProducer) Publish(_ context.Context, event commonkafka.Event) error {
	p.events = append(p.events, event)
	return p.err
}

func TestGivenPendingCheckInEvent_WhenFlushSucceeds_ThenPublishesCanonicalEventAndMarksPublished(t *testing.T) {
	// Given
	payload, err := proto.Marshal(&eventsv1.CheckInRecordedEvent{
		MemberId:    "member",
		GymId:       gymID,
		CheckedInAt: timestamppb.New(time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC)),
	})
	if err != nil {
		t.Fatal("marshal Check-in event failed")
	}
	store := &relayStore{events: []domain.OutboxEvent{{ID: "event", Topic: "checkin.recorded.v1", Key: "member", Payload: payload}}}
	producer := &relayProducer{}
	relay := kafkaadapter.NewRelay(store, producer, "ms-gym-checkin")

	// When
	err = relay.Flush(context.Background())

	// Then
	if err != nil {
		t.Fatal("flush relay failed")
	}
	if len(producer.events) != 1 || string(producer.events[0].Key) != "member" || producer.events[0].Source != "ms-gym-checkin" {
		t.Fatal("relay did not publish canonical event")
	}
	if len(store.published) != 1 || store.published[0] != "event" || len(store.retried) != 0 {
		t.Fatal("relay did not mark event published")
	}
}

func TestGivenPublishFailure_WhenFlushRuns_ThenRetainsEventForRetry(t *testing.T) {
	// Given
	payload, err := proto.Marshal(&eventsv1.CheckInRecordedEvent{MemberId: "member", GymId: gymID, CheckedInAt: timestamppb.Now()})
	if err != nil {
		t.Fatal("marshal retry Check-in event failed")
	}
	store := &relayStore{events: []domain.OutboxEvent{{ID: "event", Topic: "checkin.recorded.v1", Key: "member", Payload: payload}}}
	producer := &relayProducer{err: errors.New("broker unavailable")}
	relay := kafkaadapter.NewRelay(store, producer, "ms-gym-checkin")

	// When
	err = relay.Flush(context.Background())

	// Then
	if err != nil {
		t.Fatal("flush retry relay failed")
	}
	if len(store.retried) != 1 || store.retried[0] != "event" || len(store.published) != 0 {
		t.Fatal("relay did not retain event for retry")
	}
}
