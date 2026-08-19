package unit

import (
	"context"
	"encoding/json"
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
	events       []domain.OutboxEvent
	published    []string
	retried      []string
	failed       []string
	invalid      []string
	attempts     []int
	prepared     int
	prepareError error
	claimError   error
}

func (s *relayStore) ClaimOutbox(context.Context, int, time.Time) ([]domain.OutboxEvent, error) {
	return s.events, s.claimError
}
func (s *relayStore) SavePreparedOutbox(_ context.Context, id string, value, headers []byte) error {
	s.prepared++
	if s.prepareError != nil {
		return s.prepareError
	}
	for index := range s.events {
		if s.events[index].ID == id {
			s.events[index].PreparedValue = append([]byte(nil), value...)
			s.events[index].PreparedHeaders = append([]byte(nil), headers...)
		}
	}
	return nil
}
func (s *relayStore) MarkPublished(_ context.Context, id string, _ time.Time) error {
	s.published = append(s.published, id)
	return nil
}
func (s *relayStore) MarkRetry(_ context.Context, id string, attempts int, _ time.Time) error {
	s.retried = append(s.retried, id)
	s.attempts = append(s.attempts, attempts)
	return nil
}
func (s *relayStore) MarkFailed(_ context.Context, id string, attempts int) error {
	s.failed = append(s.failed, id)
	s.attempts = append(s.attempts, attempts)
	return nil
}
func (s *relayStore) MarkInvalid(_ context.Context, id string, attempts int) error {
	s.invalid = append(s.invalid, id)
	s.attempts = append(s.attempts, attempts)
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

type relayEncoder struct {
	frame []byte
	err   error
}

func (e relayEncoder) Encode(string, proto.Message) ([]byte, error) {
	return append([]byte(nil), e.frame...), e.err
}

type relayPublisher struct {
	records []commonkafka.RawRecord
	errors  []error
}

func (p *relayPublisher) PublishRaw(_ context.Context, record commonkafka.RawRecord) error {
	p.records = append(p.records, record)
	if len(p.errors) == 0 {
		return nil
	}
	err := p.errors[0]
	p.errors = p.errors[1:]
	return err
}

func TestGivenPendingCheckInEvent_WhenFlushSucceeds_ThenPublishesFramedRecordAndMarksPublished(t *testing.T) {
	// Given
	store := &relayStore{events: []domain.OutboxEvent{relayEvent(t, 0)}}
	publisher := &relayPublisher{}
	relay := kafkaadapter.NewRelay(store, relayEncoder{frame: []byte{0, 1, 2}}, publisher, "ms-gym-checkin", time.Second)

	// When
	err := relay.Flush(context.Background())

	// Then
	if err != nil {
		t.Fatal("flush relay failed")
	}
	if len(publisher.records) != 1 || publisher.records[0].Topic != "checkin.recorded.v1" || string(publisher.records[0].Key) != "member" || string(publisher.records[0].Value) != string([]byte{0, 1, 2}) {
		t.Fatal("relay did not publish original framed record")
	}
	if err := commonkafka.ValidateCanonicalHeaders(publisher.records[0].Headers); err != nil {
		t.Fatal("relay did not publish canonical headers")
	}
	if store.prepared != 1 || len(store.published) != 1 || len(store.retried) != 0 || len(store.failed) != 0 {
		t.Fatal("relay did not persist and mark event published")
	}
}

func TestGivenPreparedRecordPersistenceFailure_WhenFlushRuns_ThenDoesNotPublish(t *testing.T) {
	// Given
	store := &relayStore{
		events:       []domain.OutboxEvent{relayEvent(t, 0)},
		prepareError: errors.New("database unavailable"),
	}
	publisher := &relayPublisher{}
	relay := kafkaadapter.NewRelay(store, relayEncoder{frame: []byte{0, 1, 2}}, publisher, "ms-gym-checkin", time.Second)

	// When
	err := relay.Flush(context.Background())

	// Then
	if err == nil || len(publisher.records) != 0 || len(store.published) != 0 {
		t.Fatal("relay published before prepared record was durable")
	}
}

func TestGivenFirstOrSecondPublishFailure_WhenFlushRuns_ThenPersistsNextAttemptForRetry(t *testing.T) {
	for _, previousAttempts := range []int{0, 1} {
		// Given
		store := &relayStore{events: []domain.OutboxEvent{relayEvent(t, previousAttempts)}}
		publisher := &relayPublisher{errors: []error{errors.New("broker unavailable")}}
		relay := kafkaadapter.NewRelay(store, relayEncoder{frame: []byte{0, 1, 2}}, publisher, "ms-gym-checkin", time.Second)

		// When
		err := relay.Flush(context.Background())

		// Then
		if err != nil || len(store.retried) != 1 || store.attempts[0] != previousAttempts+1 || len(store.failed) != 0 {
			t.Fatalf("attempt %d was not retained for retry", previousAttempts+1)
		}
	}
}

func TestGivenThirdPublishFailure_WhenDLQIsAcknowledged_ThenPreservesRecordAndMarksFailed(t *testing.T) {
	// Given
	store := &relayStore{events: []domain.OutboxEvent{relayEvent(t, 2)}}
	publisher := &relayPublisher{errors: []error{errors.New("broker unavailable"), nil}}
	relay := kafkaadapter.NewRelay(store, relayEncoder{frame: []byte{0, 1, 2}}, publisher, "ms-gym-checkin", time.Second)

	// When
	err := relay.Flush(context.Background())

	// Then
	if err != nil || len(publisher.records) != 2 || publisher.records[1].Topic != "checkin.recorded.v1.DLQ" {
		t.Fatal("relay did not publish acknowledged DLQ record")
	}
	if string(publisher.records[1].Key) != string(publisher.records[0].Key) || string(publisher.records[1].Value) != string(publisher.records[0].Value) {
		t.Fatal("DLQ did not preserve original key and frame")
	}
	if len(store.failed) != 1 || store.attempts[0] != 3 || len(store.retried) != 0 {
		t.Fatal("relay did not mark terminal event failed")
	}
}

func TestGivenDLQFailure_WhenThirdPublishFails_ThenLeavesEventRetryable(t *testing.T) {
	// Given
	store := &relayStore{events: []domain.OutboxEvent{relayEvent(t, 2)}}
	publisher := &relayPublisher{errors: []error{errors.New("primary unavailable"), errors.New("DLQ unavailable")}}
	relay := kafkaadapter.NewRelay(store, relayEncoder{frame: []byte{0, 1, 2}}, publisher, "ms-gym-checkin", time.Second)

	// When
	err := relay.Flush(context.Background())

	// Then
	if err == nil || len(store.retried) != 1 || store.attempts[0] != 3 || len(store.failed) != 0 {
		t.Fatal("DLQ failure did not leave source event retryable")
	}
}

func TestGivenPriorDLQFailure_WhenRetryRuns_ThenSkipsPrimaryAndRetriesDLQ(t *testing.T) {
	// Given
	event := preparedRelayEvent(t, 3, []byte{0, 9, 8})
	store := &relayStore{events: []domain.OutboxEvent{event}}
	publisher := &relayPublisher{}
	relay := kafkaadapter.NewRelay(store, relayEncoder{err: errors.New("must not re-encode")}, publisher, "ms-gym-checkin", time.Second)

	// When
	err := relay.Flush(context.Background())

	// Then
	if err != nil || len(publisher.records) != 1 || publisher.records[0].Topic != "checkin.recorded.v1.DLQ" || len(store.failed) != 1 {
		t.Fatal("relay retried primary after DLQ publication failure")
	}
	if string(publisher.records[0].Value) != string(event.PreparedValue) {
		t.Fatal("relay rebuilt prepared frame before retrying DLQ")
	}
	var dlqEventID string
	for _, header := range publisher.records[0].Headers {
		if header.Key == commonkafka.HeaderEventID {
			dlqEventID = string(header.Value)
		}
	}
	if dlqEventID != "event" {
		t.Fatal("relay rebuilt prepared canonical headers before retrying DLQ")
	}
}

func TestGivenPreparationFailure_WhenFlushRuns_ThenRetriesBeforeMarkingEventInvalid(t *testing.T) {
	for _, previousAttempts := range []int{0, 1, 2} {
		// Given
		event := relayEvent(t, previousAttempts)
		event.Payload = []byte{0xff}
		store := &relayStore{events: []domain.OutboxEvent{event}}
		publisher := &relayPublisher{}
		relay := kafkaadapter.NewRelay(store, relayEncoder{frame: []byte{0, 1, 2}}, publisher, "ms-gym-checkin", time.Second)

		// When
		err := relay.Flush(context.Background())

		// Then
		if err != nil || len(publisher.records) != 0 || store.attempts[0] != previousAttempts+1 {
			t.Fatalf("preparation attempt %d did not stop before publication", previousAttempts+1)
		}
		if previousAttempts < 2 && (len(store.retried) != 1 || len(store.invalid) != 0) {
			t.Fatalf("preparation attempt %d was not retained for retry", previousAttempts+1)
		}
		if previousAttempts == 2 && (len(store.invalid) != 1 || len(store.failed) != 0) {
			t.Fatal("bounded preparation failure was not marked invalid")
		}
	}
}

func TestGivenCancelledContextBetweenClaimedEvents_WhenFlushRuns_ThenStopsBeforeNextPublish(t *testing.T) {
	// Given
	ctx, cancel := context.WithCancel(context.Background())
	store := &relayStore{events: []domain.OutboxEvent{relayEvent(t, 0), relayEvent(t, 0)}}
	publisher := &relayPublisher{}
	encoder := cancelingEncoder{cancel: cancel, frame: []byte{0, 1, 2}}
	relay := kafkaadapter.NewRelay(store, encoder, publisher, "ms-gym-checkin", time.Second)

	// When
	err := relay.Flush(ctx)

	// Then
	if !errors.Is(err, context.Canceled) || len(publisher.records) != 1 {
		t.Fatal("relay did not stop between claimed events")
	}
}

type cancelingEncoder struct {
	cancel context.CancelFunc
	frame  []byte
}

func (e cancelingEncoder) Encode(string, proto.Message) ([]byte, error) {
	e.cancel()
	return append([]byte(nil), e.frame...), nil
}

func relayEvent(t *testing.T, attempts int) domain.OutboxEvent {
	t.Helper()
	payload, err := proto.Marshal(&eventsv1.CheckInRecordedEvent{
		MemberId:    "member",
		GymId:       gymID,
		CheckedInAt: timestamppb.New(time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC)),
	})
	if err != nil {
		t.Fatal("marshal Check-in event failed")
	}
	return domain.OutboxEvent{ID: "event", Topic: "checkin.recorded.v1", Key: "member", Payload: payload, Attempts: attempts}
}

func preparedRelayEvent(t *testing.T, attempts int, frame []byte) domain.OutboxEvent {
	t.Helper()
	event := relayEvent(t, attempts)
	headers, err := commonkafka.CanonicalHeaders(
		context.Background(),
		&eventsv1.CheckInRecordedEvent{},
		"ms-gym-checkin",
		event.ID,
		time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC),
		nil,
	)
	if err != nil {
		t.Fatal("create prepared headers failed")
	}
	encodedHeaders, err := json.Marshal(headers)
	if err != nil {
		t.Fatal("marshal prepared headers failed")
	}
	event.PreparedValue = append([]byte(nil), frame...)
	event.PreparedHeaders = encodedHeaders
	return event
}
