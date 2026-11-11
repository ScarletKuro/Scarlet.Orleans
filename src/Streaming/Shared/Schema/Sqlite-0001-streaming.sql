-- Times that queries compare (visible_on, expires_on, remove_on, evict_on) are UTC ticks, which compare exactly on every
-- engine and driver; created_on and dead_on are only read by people. AUTOINCREMENT keeps SQLite from reusing the ID of a
-- deleted last message, which a receipt for the old message would then match.
CREATE TABLE IF NOT EXISTS orleans_stream_queue (
    queue_hash BLOB NOT NULL,
    queue_identity BLOB NOT NULL,
    service_id TEXT NOT NULL,
    provider_name TEXT NOT NULL,
    queue_id TEXT NOT NULL,
    evict_on INTEGER NOT NULL,
    created_on TEXT NOT NULL,
    CONSTRAINT pk_orleans_stream_queue PRIMARY KEY (queue_hash));

CREATE TABLE IF NOT EXISTS orleans_stream_message (
    message_id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_hash BLOB NOT NULL,
    message_key BLOB NOT NULL,
    dequeued INTEGER NOT NULL,
    visible_on INTEGER NOT NULL,
    expires_on INTEGER NOT NULL,
    created_on TEXT NOT NULL,
    payload BLOB NOT NULL,
    CONSTRAINT uq_orleans_stream_message_key UNIQUE (message_key));

CREATE UNIQUE INDEX IF NOT EXISTS ix_orleans_stream_message_queue ON orleans_stream_message (queue_hash, message_id);

CREATE TABLE IF NOT EXISTS orleans_stream_dead_letter (
    queue_hash BLOB NOT NULL,
    message_id INTEGER NOT NULL,
    message_key BLOB NOT NULL,
    dequeued INTEGER NOT NULL,
    expires_on INTEGER NOT NULL,
    created_on TEXT NOT NULL,
    payload BLOB NOT NULL,
    dead_on TEXT NOT NULL,
    remove_on INTEGER NOT NULL,
    CONSTRAINT pk_orleans_stream_dead_letter PRIMARY KEY (queue_hash, message_id));

CREATE INDEX IF NOT EXISTS ix_orleans_stream_dead_letter_remove_on ON orleans_stream_dead_letter (queue_hash, remove_on);

-- Records this script in the schema history, so a database set up by hand passes the provider's startup validation.
CREATE TABLE IF NOT EXISTS orleans_schema_history (
    component TEXT NOT NULL,
    version INTEGER NOT NULL,
    script TEXT NOT NULL,
    checksum TEXT NULL,
    source TEXT NOT NULL,
    applied_on TEXT NOT NULL,
    CONSTRAINT pk_orleans_schema_history PRIMARY KEY (component, version));
INSERT INTO orleans_schema_history (component, version, script, checksum, source, applied_on)
SELECT 'streaming', 1, 'Sqlite-0001-streaming.sql', NULL, 'manual', CURRENT_TIMESTAMP
WHERE NOT EXISTS (SELECT 1 FROM orleans_schema_history WHERE component = 'streaming' AND version = 1);
