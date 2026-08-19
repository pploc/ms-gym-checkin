package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	commonkafka "github.com/pploc/common-go/kafka"
	eventsv1 "github.com/pploc/proto-go/events/v1"
	"google.golang.org/protobuf/proto"

	"github.com/pploc/ms-gym-checkin/internal/domain"
	"github.com/pploc/ms-gym-checkin/internal/usecase/port"
)

const maxPublishFailures = 3

type Relay struct {
	store      port.Store
	encoder    commonkafka.FrameEncoder
	publisher  commonkafka.RawPublisher
	source     string
	retryDelay time.Duration
}

func NewRelay(store port.Store, encoder commonkafka.FrameEncoder, publisher commonkafka.RawPublisher, source string, retryDelay time.Duration) *Relay {
	return &Relay{store: store, encoder: encoder, publisher: publisher, source: source, retryDelay: retryDelay}
}

func (r *Relay) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := r.Flush(ctx); err != nil && ctx.Err() == nil {
			log.Print("outbox relay flush failed")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *Relay) Flush(ctx context.Context) error {
	now := time.Now().UTC()
	events, err := r.store.ClaimOutbox(ctx, 50, now)
	if err != nil {
		return err
	}
	for _, event := range events {
		if err := ctx.Err(); err != nil {
			return err
		}
		record, err := r.record(ctx, event, now)
		if err != nil {
			attempts := event.Attempts + 1
			if attempts >= maxPublishFailures {
				if markErr := r.store.MarkInvalid(ctx, event.ID, attempts); markErr != nil {
					return markErr
				}
				continue
			}
			if markErr := r.store.MarkRetry(ctx, event.ID, attempts, now.Add(r.retryDelay)); markErr != nil {
				return markErr
			}
			continue
		}
		if event.PreparedValue == nil {
			encodedHeaders, err := json.Marshal(record.Headers)
			if err != nil {
				return err
			}
			if err := r.store.SavePreparedOutbox(ctx, event.ID, record.Value, encodedHeaders); err != nil {
				return err
			}
		}
		if event.Attempts >= maxPublishFailures {
			if err := r.publishDLQ(ctx, event, record, event.Attempts, now); err != nil {
				return err
			}
			continue
		}
		if err := r.publisher.PublishRaw(ctx, record); err == nil {
			if err := r.store.MarkPublished(ctx, event.ID, now); err != nil {
				return err
			}
			continue
		}
		attempts := event.Attempts + 1
		if attempts < maxPublishFailures {
			if err := r.store.MarkRetry(ctx, event.ID, attempts, now.Add(r.retryDelay)); err != nil {
				return err
			}
			continue
		}
		if err := r.publishDLQ(ctx, event, record, attempts, now); err != nil {
			return err
		}
	}
	return nil
}

func (r *Relay) record(ctx context.Context, event domain.OutboxEvent, now time.Time) (commonkafka.RawRecord, error) {
	if event.Topic != "checkin.recorded.v1" {
		return commonkafka.RawRecord{}, fmt.Errorf("unsupported outbox topic")
	}
	if event.PreparedValue != nil || event.PreparedHeaders != nil {
		if len(event.PreparedValue) == 0 || len(event.PreparedHeaders) == 0 {
			return commonkafka.RawRecord{}, fmt.Errorf("incomplete prepared outbox record")
		}
		var headers []commonkafka.Header
		if err := json.Unmarshal(event.PreparedHeaders, &headers); err != nil {
			return commonkafka.RawRecord{}, fmt.Errorf("decode prepared outbox headers: %w", err)
		}
		if err := commonkafka.ValidateCanonicalHeaders(headers); err != nil {
			return commonkafka.RawRecord{}, err
		}
		return commonkafka.RawRecord{Topic: event.Topic, Key: []byte(event.Key), Value: event.PreparedValue, Headers: headers}, nil
	}
	message := &eventsv1.CheckInRecordedEvent{}
	if err := proto.Unmarshal(event.Payload, message); err != nil {
		return commonkafka.RawRecord{}, err
	}
	publication := commonkafka.Event{Topic: event.Topic, Key: []byte(event.Key), Payload: message, EventID: event.ID, Source: r.source}
	if err := commonkafka.ValidateEvent(publication); err != nil {
		return commonkafka.RawRecord{}, err
	}
	headers, err := commonkafka.CanonicalHeaders(ctx, message, r.source, event.ID, now, nil)
	if err != nil {
		return commonkafka.RawRecord{}, err
	}
	frame, err := r.encoder.Encode(event.Topic, message)
	if err != nil {
		return commonkafka.RawRecord{}, err
	}
	return commonkafka.RawRecord{Topic: event.Topic, Key: []byte(event.Key), Value: frame, Headers: headers}, nil
}

func (r *Relay) publishDLQ(ctx context.Context, event domain.OutboxEvent, record commonkafka.RawRecord, attempts int, now time.Time) error {
	if err := r.publisher.PublishRaw(ctx, commonkafka.DLQRecord(record, "Kafka message publication failed", attempts, now)); err != nil {
		if retryErr := r.store.MarkRetry(ctx, event.ID, attempts, now.Add(r.retryDelay)); retryErr != nil {
			return retryErr
		}
		return err
	}
	return r.store.MarkFailed(ctx, event.ID, attempts)
}
