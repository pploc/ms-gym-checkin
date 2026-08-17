//go:build integration

package integration

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	commonkafka "github.com/pploc/common-go/kafka"
	eventsv1 "github.com/pploc/proto-go/events/v1"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestGivenSeededRegistry_WhenPublishingCheckInEvent_ThenBrokerGetsConfluentFrameAndCanonicalMetadata(t *testing.T) {
	// Given
	broker, registryURL := kafkaEnvironment(t)
	registry, err := commonkafka.NewConfluentProtobufRegistry(commonkafka.RegistryConfig{URL: registryURL})
	if err != nil {
		t.Fatal("create Schema Registry client failed")
	}
	t.Cleanup(func() { _ = registry.Close() })
	producer, err := commonkafka.NewFranzProducer(commonkafka.TransportConfig{Brokers: []string{broker}, PublishTimeout: 5 * time.Second}, registry)
	if err != nil {
		t.Fatal("create Kafka producer failed")
	}
	t.Cleanup(producer.Close)
	consumer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics("checkin.recorded.v1"), kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	if err != nil {
		t.Fatal("create Kafka consumer failed")
	}
	t.Cleanup(consumer.Close)
	time.Sleep(100 * time.Millisecond)
	memberID := "integration-member"
	message := &eventsv1.CheckInRecordedEvent{MemberId: memberID, GymId: testGymID, CheckedInAt: timestamppb.New(time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC))}

	// When
	if err := producer.Publish(context.Background(), commonkafka.Event{Topic: "checkin.recorded.v1", Key: []byte(memberID), Payload: message, EventID: "integration-event", Source: "ms-gym-checkin"}); err != nil {
		t.Fatal("publish Check-in event failed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fetches := consumer.PollFetches(ctx)
	if err := fetches.Err(); err != nil {
		t.Fatal("poll Kafka record failed")
	}
	var record *kgo.Record
	fetches.EachRecord(func(value *kgo.Record) { record = value })

	// Then
	if record == nil {
		t.Fatal("expected Kafka record")
	}
	if string(record.Key) != memberID || len(record.Value) < 6 || record.Value[0] != 0 {
		t.Fatal("unexpected Kafka key or Confluent frame")
	}
	if binary.BigEndian.Uint32(record.Value[1:5]) == 0 || record.Value[5] != 0 {
		t.Fatal("unexpected Schema Registry frame")
	}
	if header(record, "event-type") != "events.v1.CheckInRecordedEvent" || header(record, "source") != "ms-gym-checkin" || header(record, "event-id") != "integration-event" {
		t.Fatal("unexpected canonical Kafka headers")
	}
}

func TestGivenReachableUnseededRegistry_WhenPublishingCheckInEvent_ThenLookupOnlyProducerFails(t *testing.T) {
	// Given
	broker, _ := kafkaEnvironment(t)
	registryURL := emptyRegistry(t)
	registry, err := commonkafka.NewConfluentProtobufRegistry(commonkafka.RegistryConfig{URL: registryURL})
	if err != nil {
		t.Fatal("create empty Schema Registry client failed")
	}
	t.Cleanup(func() { _ = registry.Close() })
	producer, err := commonkafka.NewFranzProducer(commonkafka.TransportConfig{Brokers: []string{broker}, PublishTimeout: time.Second}, registry)
	if err != nil {
		t.Fatal("create producer for empty Registry lookup test failed")
	}
	t.Cleanup(producer.Close)

	// When
	err = producer.Publish(context.Background(), commonkafka.Event{Topic: "checkin.recorded.v1", Key: []byte("member"), Payload: &eventsv1.CheckInRecordedEvent{}, EventID: "event", Source: "ms-gym-checkin"})

	// Then
	if err == nil {
		t.Fatal("expected lookup-only Schema Registry failure")
	}
}

func kafkaEnvironment(t *testing.T) (string, string) {
	t.Helper()
	broker, registry := os.Getenv("KAFKA_BROKERS"), os.Getenv("SCHEMA_REGISTRY_URL")
	if broker == "" || registry == "" {
		t.Skip("KAFKA_BROKERS and SCHEMA_REGISTRY_URL not set; run make start-env and seed fixtures")
	}
	return broker, registry
}

func emptyRegistry(t *testing.T) string {
	t.Helper()
	url := os.Getenv("UNSEEDED_SCHEMA_REGISTRY_URL")
	if url == "" {
		t.Skip("UNSEEDED_SCHEMA_REGISTRY_URL not set; use a reachable disposable empty Schema Registry")
	}
	response, err := http.Get(url + "/subjects")
	if err != nil {
		t.Fatal("query empty Schema Registry failed")
	}
	t.Cleanup(func() { _ = response.Body.Close() })
	if response.StatusCode != http.StatusOK {
		t.Fatal("empty Schema Registry did not return success")
	}
	var subjects []string
	if err := json.NewDecoder(response.Body).Decode(&subjects); err != nil {
		t.Fatal("decode empty Schema Registry subjects failed")
	}
	if len(subjects) != 0 {
		t.Fatal("Schema Registry must be empty")
	}
	return url
}

func header(record *kgo.Record, key string) string {
	for _, candidate := range record.Headers {
		if candidate.Key == key {
			return string(candidate.Value)
		}
	}
	return ""
}
