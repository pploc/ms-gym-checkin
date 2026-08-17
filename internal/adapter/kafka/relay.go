package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	commonkafka "github.com/pploc/common-go/kafka"
	eventsv1 "github.com/pploc/proto-go/events/v1"
	"google.golang.org/protobuf/proto"

	"github.com/pploc/ms-gym-checkin/internal/domain"
	"github.com/pploc/ms-gym-checkin/internal/usecase/port"
)

type Relay struct {
	store    port.Store
	producer commonkafka.Producer
	source   string
}

func NewRelay(store port.Store, producer commonkafka.Producer, source string) *Relay {
	return &Relay{store: store, producer: producer, source: source}
}
func (r *Relay) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := r.Flush(ctx); err != nil {
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
	events, err := r.store.ClaimOutbox(ctx, 50, time.Now().UTC())
	if err != nil {
		return err
	}
	for _, event := range events {
		if err := r.publish(ctx, event); err != nil {
			if markErr := r.store.MarkRetry(ctx, event.ID, time.Now().UTC()); markErr != nil {
				return markErr
			}
			continue
		}
		if err := r.store.MarkPublished(ctx, event.ID, time.Now().UTC()); err != nil {
			return err
		}
	}
	return nil
}
func (r *Relay) publish(ctx context.Context, event domain.OutboxEvent) error {
	if event.Topic != "checkin.recorded.v1" {
		return fmt.Errorf("unsupported outbox topic")
	}
	message := &eventsv1.CheckInRecordedEvent{}
	if err := proto.Unmarshal(event.Payload, message); err != nil {
		return err
	}
	return r.producer.Publish(ctx, commonkafka.Event{Topic: event.Topic, Key: []byte(event.Key), Payload: message, EventID: event.ID, Source: r.source})
}
