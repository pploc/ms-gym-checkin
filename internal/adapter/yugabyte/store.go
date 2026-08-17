package yugabyte

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/pploc/ms-gym-checkin/internal/domain"
)

type Store struct{ db *sql.DB }

func New(databaseURL string) (*Store, error) {
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }
func (s *Store) Close() error                   { return s.db.Close() }

func (s *Store) FindIdempotency(ctx context.Context, userID, idempotencyKey string) (*domain.IdempotencyResult, error) {
	row := s.db.QueryRowContext(ctx, `SELECT request_fingerprint, checkin_id, member_id, gym_id, checked_in_at FROM check_ins WHERE user_id = $1 AND idempotency_key = $2`, userID, idempotencyKey)
	var result domain.IdempotencyResult
	var record domain.CheckInRecord
	if err := row.Scan(&result.Fingerprint, &record.ID, &record.MemberID, &record.GymID, &record.CheckedInAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	record.UserID = userID
	result.Record = record
	return &result, nil
}

func (s *Store) CreateCheckIn(ctx context.Context, record domain.CheckInRecord, fingerprint, idempotencyKey string, event domain.OutboxEvent) (domain.CheckInRecord, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.CheckInRecord{}, err
	}
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, `INSERT INTO check_ins (checkin_id, user_id, member_id, gym_id, idempotency_key, request_fingerprint, checked_in_at) VALUES ($1,$2,$3,$4,$5,$6,$7)`, record.ID, record.UserID, record.MemberID, record.GymID, idempotencyKey, fingerprint, record.CheckedInAt)
	if err != nil {
		if isUnique(err) {
			var result domain.IdempotencyResult
			row := tx.QueryRowContext(ctx, `SELECT request_fingerprint, checkin_id, member_id, gym_id, checked_in_at FROM check_ins WHERE user_id = $1 AND idempotency_key = $2`, record.UserID, idempotencyKey)
			if scanErr := row.Scan(&result.Fingerprint, &result.Record.ID, &result.Record.MemberID, &result.Record.GymID, &result.Record.CheckedInAt); scanErr != nil {
				return domain.CheckInRecord{}, scanErr
			}
			result.Record.UserID = record.UserID
			if result.Fingerprint != fingerprint {
				return domain.CheckInRecord{}, fmt.Errorf("idempotency conflict")
			}
			return result.Record, nil
		}
		return domain.CheckInRecord{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox_events (event_id, topic, message_key, payload, status, attempts, created_at, available_at) VALUES ($1,$2,$3,$4,'PENDING',0,$5,$5)`, event.ID, event.Topic, event.Key, event.Payload, event.CreatedAt)
	if err != nil {
		return domain.CheckInRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.CheckInRecord{}, err
	}
	return record, nil
}

func (s *Store) ListByUser(ctx context.Context, userID string, page, limit int) ([]domain.CheckInRecord, int, error) {
	return s.list(ctx, `user_id = $1`, userID, page, limit)
}
func (s *Store) ListByMember(ctx context.Context, memberID string, page, limit int) ([]domain.CheckInRecord, int, error) {
	return s.list(ctx, `member_id = $1`, memberID, page, limit)
}
func (s *Store) list(ctx context.Context, where, value string, page, limit int) ([]domain.CheckInRecord, int, error) {
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM check_ins WHERE `+where, value).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT checkin_id, user_id, member_id, gym_id, checked_in_at FROM check_ins WHERE `+where+` ORDER BY checked_in_at DESC, checkin_id DESC LIMIT $2 OFFSET $3`, value, limit, page*limit)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var records []domain.CheckInRecord
	for rows.Next() {
		var record domain.CheckInRecord
		if err := rows.Scan(&record.ID, &record.UserID, &record.MemberID, &record.GymID, &record.CheckedInAt); err != nil {
			return nil, 0, err
		}
		records = append(records, record)
	}
	return records, total, rows.Err()
}

func (s *Store) DailyCount(ctx context.Context, gymID string, start, end time.Time) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM check_ins WHERE gym_id = $1 AND checked_in_at >= $2 AND checked_in_at < $3`, gymID, start, end).Scan(&count)
	return count, err
}

func (s *Store) CurrentKey(ctx context.Context, gymID string) (*domain.RootKey, error) {
	return s.key(ctx, `gym_id = $1 AND status = 'CURRENT'`, gymID)
}
func (s *Store) FindKey(ctx context.Context, gymID string, version uint64) (*domain.RootKey, error) {
	return s.key(ctx, `gym_id = $1 AND key_version = $2`, gymID, version)
}
func (s *Store) key(ctx context.Context, where string, values ...any) (*domain.RootKey, error) {
	row := s.db.QueryRowContext(ctx, `SELECT gym_id, key_version, vault_ciphertext, vault_key_reference, status, activated_at, acceptance_deadline, retired_at FROM gym_qr_root_keys WHERE `+where, values...)
	var key domain.RootKey
	var deadline sql.NullTime
	var retired sql.NullTime
	if err := row.Scan(&key.GymID, &key.Version, &key.Ciphertext, &key.VaultKeyReference, &key.Status, &key.ActivatedAt, &deadline, &retired); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if deadline.Valid {
		key.AcceptanceDeadline = deadline.Time
	}
	if retired.Valid {
		key.RetiredAt = &retired.Time
	}
	return &key, nil
}

func (s *Store) CreateCurrentKey(ctx context.Context, key domain.RootKey) (domain.RootKey, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.RootKey{}, err
	}
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, `INSERT INTO gym_qr_root_keys (gym_id, key_version, vault_ciphertext, vault_key_reference, status, activated_at) VALUES ($1,$2,$3,$4,'CURRENT',$5)`, key.GymID, key.Version, key.Ciphertext, key.VaultKeyReference, key.ActivatedAt)
	if err != nil && !isUnique(err) {
		return domain.RootKey{}, err
	}
	current, err := s.keyTx(ctx, tx, key.GymID)
	if err != nil {
		return domain.RootKey{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.RootKey{}, err
	}
	return current, nil
}

func (s *Store) RotateKey(ctx context.Context, key domain.RootKey, deadline time.Time, emergency bool) (domain.RootKey, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.RootKey{}, err
	}
	defer func() { _ = tx.Rollback() }()
	previousStatus := "PREVIOUS"
	var retiredAt any
	if emergency {
		previousStatus = "RETIRED"
		retiredAt = deadline
	}
	_, err = tx.ExecContext(ctx, `UPDATE gym_qr_root_keys SET status = $2, acceptance_deadline = $3, retired_at = $4 WHERE gym_id = $1 AND status = 'CURRENT'`, key.GymID, previousStatus, deadline, retiredAt)
	if err != nil {
		return domain.RootKey{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO gym_qr_root_keys (gym_id,key_version,vault_ciphertext,vault_key_reference,status,activated_at) VALUES ($1,$2,$3,$4,'CURRENT',$5)`, key.GymID, key.Version, key.Ciphertext, key.VaultKeyReference, key.ActivatedAt)
	if err != nil {
		return domain.RootKey{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.RootKey{}, err
	}
	return key, nil
}

func (s *Store) keyTx(ctx context.Context, tx *sql.Tx, gymID string) (domain.RootKey, error) {
	var key domain.RootKey
	err := tx.QueryRowContext(ctx, `SELECT gym_id,key_version,vault_ciphertext,vault_key_reference,status,activated_at FROM gym_qr_root_keys WHERE gym_id=$1 AND status='CURRENT'`, gymID).Scan(&key.GymID, &key.Version, &key.Ciphertext, &key.VaultKeyReference, &key.Status, &key.ActivatedAt)
	return key, err
}

func (s *Store) ClaimOutbox(ctx context.Context, limit int, now time.Time) ([]domain.OutboxEvent, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `WITH claimed AS (SELECT event_id FROM outbox_events WHERE status = 'PENDING' AND available_at <= $1 ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT $2) UPDATE outbox_events o SET status='PUBLISHING', claimed_at=$1 FROM claimed WHERE o.event_id=claimed.event_id RETURNING o.event_id,o.topic,o.message_key,o.payload,o.attempts,o.created_at`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []domain.OutboxEvent
	for rows.Next() {
		var event domain.OutboxEvent
		if err := rows.Scan(&event.ID, &event.Topic, &event.Key, &event.Payload, &event.Attempts, &event.CreatedAt); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return events, nil
}
func (s *Store) MarkPublished(ctx context.Context, id string, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE outbox_events SET status='PUBLISHED',published_at=$2 WHERE event_id=$1`, id, now)
	return err
}
func (s *Store) MarkRetry(ctx context.Context, id string, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE outbox_events SET status='PENDING',attempts=attempts+1,available_at=$2,claimed_at=NULL WHERE event_id=$1`, id, now.Add(time.Second))
	return err
}
func isUnique(err error) bool {
	return err != nil && (contains(err.Error(), "duplicate key") || contains(err.Error(), "unique constraint"))
}
func contains(value, want string) bool {
	for i := 0; i+len(want) <= len(value); i++ {
		if value[i:i+len(want)] == want {
			return true
		}
	}
	return false
}
