CREATE TABLE IF NOT EXISTS gym_qr_root_keys (
    gym_id TEXT NOT NULL,
    key_version BIGINT NOT NULL CHECK (key_version > 0),
    vault_ciphertext TEXT NOT NULL,
    vault_key_reference TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('CURRENT', 'PREVIOUS', 'RETIRED')),
    activated_at TIMESTAMPTZ NOT NULL,
    acceptance_deadline TIMESTAMPTZ,
    retired_at TIMESTAMPTZ,
    PRIMARY KEY (gym_id, key_version)
);

CREATE UNIQUE INDEX IF NOT EXISTS gym_qr_root_keys_one_current
    ON gym_qr_root_keys (gym_id)
    WHERE status = 'CURRENT';

CREATE INDEX IF NOT EXISTS gym_qr_root_keys_acceptable
    ON gym_qr_root_keys (gym_id, status, acceptance_deadline);

CREATE TABLE IF NOT EXISTS check_ins (
    checkin_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    member_id TEXT NOT NULL,
    gym_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_fingerprint TEXT NOT NULL,
    checked_in_at TIMESTAMPTZ NOT NULL,
    UNIQUE (user_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS check_ins_user_history ON check_ins (user_id, checked_in_at DESC, checkin_id DESC);
CREATE INDEX IF NOT EXISTS check_ins_member_history ON check_ins (member_id, checked_in_at DESC, checkin_id DESC);
CREATE INDEX IF NOT EXISTS check_ins_daily_count ON check_ins (gym_id, checked_in_at);

CREATE TABLE IF NOT EXISTS outbox_events (
    event_id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    message_key TEXT NOT NULL,
    payload BYTEA NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'PUBLISHING', 'PUBLISHED', 'FAILED')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    created_at TIMESTAMPTZ NOT NULL,
    available_at TIMESTAMPTZ NOT NULL,
    claimed_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS outbox_events_claimable ON outbox_events (status, available_at, created_at);
