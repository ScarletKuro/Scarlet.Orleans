CREATE TABLE IF NOT EXISTS orleans_transactional_state (
    key_hash BLOB NOT NULL,
    state_identity BLOB NOT NULL,
    service_id TEXT NOT NULL,
    state_name TEXT NOT NULL,
    grain_id TEXT NOT NULL,
    etag BLOB NOT NULL,
    committed_sequence_id INTEGER NOT NULL,
    metadata BLOB NOT NULL,
    modified_on TEXT NOT NULL,
    CONSTRAINT pk_orleans_transactional_state PRIMARY KEY (key_hash));

CREATE TABLE IF NOT EXISTS orleans_transactional_state_entry (
    key_hash BLOB NOT NULL,
    sequence_id INTEGER NOT NULL,
    transaction_id TEXT NOT NULL,
    timestamp_ticks INTEGER NOT NULL,
    transaction_manager BLOB NOT NULL,
    payload BLOB NOT NULL,
    CONSTRAINT pk_orleans_transactional_state_entry PRIMARY KEY (key_hash, sequence_id));

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
SELECT 'transactions', 1, 'Sqlite-0001-transactions.sql', NULL, 'manual', CURRENT_TIMESTAMP
WHERE NOT EXISTS (SELECT 1 FROM orleans_schema_history WHERE component = 'transactions' AND version = 1);
