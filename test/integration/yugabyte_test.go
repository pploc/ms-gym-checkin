//go:build integration

package integration

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	commonerrors "github.com/pploc/common-go/errors"

	"github.com/pploc/ms-gym-checkin/internal/adapter/yugabyte"
	"github.com/pploc/ms-gym-checkin/internal/domain"
)

const testGymID = "123e4567-e89b-12d3-a456-426614174000"

func TestGivenLiveYugabyte_WhenMigrationRuns_ThenSchemaIsAvailable(t *testing.T) {
	// Given
	store, _ := openYugabyte(t)
	var tables int

	// When
	err := store.DB().QueryRow(`SELECT COUNT(*) FROM information_schema.tables WHERE table_name='check_ins'`).Scan(&tables)

	// Then
	if err != nil || tables != 1 {
		t.Fatal("check_ins schema is unavailable")
	}
	if err := store.Ping(context.Background()); err != nil {
		t.Fatal("schema is unavailable after migration")
	}
}

func TestGivenNoCurrentKey_WhenConcurrentCreationRuns_ThenBothReturnOneCanonicalKey(t *testing.T) {
	// Given
	store, _ := openYugabyte(t)
	ctx := context.Background()
	keys := []domain.RootKey{
		rootKey("ciphertext-a", time.Now().UTC()),
		rootKey("ciphertext-b", time.Now().UTC().Add(time.Millisecond)),
	}
	results := make(chan domain.RootKey, len(keys))
	errs := make(chan error, len(keys))
	var group sync.WaitGroup

	// When
	for _, key := range keys {
		group.Add(1)
		go func(key domain.RootKey) {
			defer group.Done()
			created, err := store.CreateCurrentKey(ctx, key)
			results <- created
			errs <- err
		}(key)
	}
	group.Wait()
	close(results)
	close(errs)

	// Then
	for err := range errs {
		if err != nil {
			t.Fatal("create current key failed")
		}
	}
	var returned []domain.RootKey
	for key := range results {
		returned = append(returned, key)
	}
	if len(returned) != 2 || returned[0].Ciphertext != returned[1].Ciphertext {
		t.Fatal("concurrent callers returned different keys")
	}
	var count int
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM gym_qr_root_keys WHERE gym_id=$1 AND status='CURRENT'`, testGymID).Scan(&count); err != nil {
		t.Fatal("query current key count failed")
	}
	if count != 1 {
		t.Fatalf("current key count=%d", count)
	}
}

func TestGivenSameIdempotencyKey_WhenConcurrentEquivalentWritesRun_ThenOneRecordAndOutboxPersist(t *testing.T) {
	// Given
	store, _ := openYugabyte(t)
	ctx := context.Background()
	now := time.Now().UTC()
	results := make(chan domain.CheckInRecord, 2)
	errs := make(chan error, 2)
	var group sync.WaitGroup

	// When
	for index := 0; index < 2; index++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			record := checkInRecord(string(rune('a'+index)), now)
			event := outboxEvent(string(rune('a'+index)), now)
			created, err := store.CreateCheckIn(ctx, record, "same-fingerprint", "same-key", event)
			results <- created
			errs <- err
		}(index)
	}
	group.Wait()
	close(results)
	close(errs)

	// Then
	for err := range errs {
		if err != nil {
			t.Fatal("create check-in failed")
		}
	}
	var ids []string
	for record := range results {
		ids = append(ids, record.ID)
	}
	if len(ids) != 2 || ids[0] != ids[1] {
		t.Fatal("concurrent callers returned different records")
	}
	assertTableCount(t, store, "check_ins", 1)
	assertTableCount(t, store, "outbox_events", 1)
}

func TestGivenSameIdempotencyKey_WhenFingerprintChanges_ThenConflictDoesNotCreateOutbox(t *testing.T) {
	// Given
	store, _ := openYugabyte(t)
	ctx := context.Background()
	now := time.Now().UTC()
	_, err := store.CreateCheckIn(ctx, checkInRecord("first", now), "first-fingerprint", "same-key", outboxEvent("first", now))
	if err != nil {
		t.Fatal("create initial idempotent check-in failed")
	}

	// When
	_, err = store.CreateCheckIn(ctx, checkInRecord("second", now), "second-fingerprint", "same-key", outboxEvent("second", now))

	// Then
	var categorized *commonerrors.Error
	if !errors.As(err, &categorized) || categorized.Code != "IDEMPOTENCY_CONFLICT" {
		t.Fatal("expected IDEMPOTENCY_CONFLICT")
	}
	assertTableCount(t, store, "check_ins", 1)
	assertTableCount(t, store, "outbox_events", 1)
}

func TestGivenOutboxInsertConflict_WhenCheckInTransactionRuns_ThenRecordRollsBack(t *testing.T) {
	// Given
	store, _ := openYugabyte(t)
	ctx := context.Background()
	now := time.Now().UTC()
	_, err := store.CreateCheckIn(ctx, checkInRecord("first", now), "first", "first", outboxEvent("shared", now))
	if err != nil {
		t.Fatal("create initial check-in failed")
	}

	// When
	_, err = store.CreateCheckIn(ctx, checkInRecord("second", now), "second", "second", outboxEvent("shared", now))

	// Then
	if err == nil {
		t.Fatal("expected duplicate outbox event error")
	}
	assertTableCount(t, store, "check_ins", 1)
	assertTableCount(t, store, "outbox_events", 1)
}

func TestGivenCurrentKey_WhenConcurrentRotationsRun_ThenVersionsAreSerialized(t *testing.T) {
	// Given
	store, _ := openYugabyte(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if _, err := store.CreateCurrentKey(ctx, rootKey("ciphertext-1", now)); err != nil {
		t.Fatal("create current key failed")
	}
	versions := make(chan uint64, 2)
	errs := make(chan error, 2)
	var group sync.WaitGroup

	// When
	for index := 0; index < 2; index++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			key := rootKey("ciphertext-"+string(rune('2'+index)), now.Add(time.Duration(index+1)*time.Second))
			key.Version = 2
			created, err := store.RotateKey(ctx, key, now.Add(2*time.Minute), false)
			versions <- created.Version
			errs <- err
		}(index)
	}
	group.Wait()
	close(versions)
	close(errs)

	// Then
	for err := range errs {
		if err != nil {
			t.Fatal("rotate key failed")
		}
	}
	var got []int
	for version := range versions {
		got = append(got, int(version))
	}
	sort.Ints(got)
	if len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatal("concurrent key versions were not serialized")
	}
	var current, previous, retired int
	if err := store.DB().QueryRow(`SELECT COUNT(*) FILTER (WHERE status='CURRENT'), COUNT(*) FILTER (WHERE status='PREVIOUS'), COUNT(*) FILTER (WHERE status='RETIRED') FROM gym_qr_root_keys WHERE gym_id=$1`, testGymID).Scan(&current, &previous, &retired); err != nil {
		t.Fatal("query key state counts failed")
	}
	if current != 1 || previous != 1 || retired != 1 {
		t.Fatalf("current=%d previous=%d retired=%d", current, previous, retired)
	}
}

func TestGivenStalePublishingOutbox_WhenClaimRuns_ThenEventIsRecovered(t *testing.T) {
	// Given
	store, _ := openYugabyte(t)
	ctx := context.Background()
	now := time.Now().UTC()
	_, err := store.CreateCheckIn(ctx, checkInRecord("record", now), "fingerprint", "key", outboxEvent("event", now))
	if err != nil {
		t.Fatal("create check-in for stale outbox test failed")
	}
	if _, err := store.ClaimOutbox(ctx, 1, now); err != nil {
		t.Fatal("claim pending outbox event failed")
	}

	// When
	events, err := store.ClaimOutbox(ctx, 1, now.Add(2*time.Minute))

	// Then
	if err != nil {
		t.Fatal("reclaim stale outbox event failed")
	}
	if len(events) != 1 || events[0].ID != "event" {
		t.Fatal("stale outbox event was not recovered")
	}
}

func openYugabyte(t *testing.T) (*yugabyte.Store, string) {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; run make start-env")
	}
	store, err := yugabyte.New(dsn)
	if err != nil {
		t.Fatal("open Yugabyte failed")
	}
	t.Cleanup(func() { _ = store.Close() })
	migrationPath := filepath.Join("..", "..", "migrations", "001_init.sql")
	migration, err := os.ReadFile(migrationPath)
	if err != nil {
		t.Fatal("read migration failed")
	}
	if err := yugabyte.Migrate(context.Background(), store.DB(), string(migration)); err != nil {
		t.Fatal("migration failed")
	}
	t.Cleanup(func() {
		_, _ = store.DB().Exec(`DROP TABLE IF EXISTS outbox_events, check_ins, gym_qr_root_keys CASCADE`)
	})
	return store, string(migration)
}

func rootKey(ciphertext string, activatedAt time.Time) domain.RootKey {
	return domain.RootKey{GymID: testGymID, Version: 1, Ciphertext: ciphertext, VaultKeyReference: "checkin-root", Status: domain.RootKeyCurrent, ActivatedAt: activatedAt}
}

func checkInRecord(id string, checkedInAt time.Time) domain.CheckInRecord {
	return domain.CheckInRecord{ID: id, UserID: "test-user", MemberID: "test-member", GymID: testGymID, CheckedInAt: checkedInAt}
}

func outboxEvent(id string, createdAt time.Time) domain.OutboxEvent {
	return domain.OutboxEvent{ID: id, Topic: "checkin.recorded.v1", Key: "test-member", Payload: []byte{1}, CreatedAt: createdAt}
}

func assertTableCount(t *testing.T, store *yugabyte.Store, table string, want int) {
	t.Helper()
	var got int
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&got); err != nil {
		t.Fatal("query table count failed")
	}
	if got != want {
		t.Fatalf("%s count=%d want=%d", table, got, want)
	}
}
