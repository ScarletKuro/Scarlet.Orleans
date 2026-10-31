CREATE TABLE IF NOT EXISTS orleans_transactional_state (
    key_hash bytea NOT NULL,
    state_identity bytea NOT NULL,
    service_id text NOT NULL,
    state_name text NOT NULL,
    grain_id text NOT NULL,
    etag bytea NOT NULL,
    committed_sequence_id bigint NOT NULL,
    metadata bytea NOT NULL,
    modified_on timestamp with time zone NOT NULL,
    CONSTRAINT pk_orleans_transactional_state PRIMARY KEY (key_hash));

CREATE TABLE IF NOT EXISTS orleans_transactional_state_entry (
    key_hash bytea NOT NULL,
    sequence_id bigint NOT NULL,
    transaction_id text NOT NULL,
    timestamp_ticks bigint NOT NULL,
    transaction_manager bytea NOT NULL,
    payload bytea NOT NULL,
    CONSTRAINT pk_orleans_transactional_state_entry PRIMARY KEY (key_hash, sequence_id));

-- Records this script in the schema history, so a database set up by hand passes the provider's startup validation.
CREATE TABLE IF NOT EXISTS orleans_schema_history (
    component varchar(128) NOT NULL,
    version integer NOT NULL,
    script varchar(256) NOT NULL,
    checksum varchar(64) NULL,
    source varchar(16) NOT NULL,
    applied_on timestamp with time zone NOT NULL,
    CONSTRAINT pk_orleans_schema_history PRIMARY KEY (component, version));
INSERT INTO orleans_schema_history (component, version, script, checksum, source, applied_on)
SELECT 'transactions', 1, 'PostgreSql-0001-transactions.sql', NULL, 'manual', CURRENT_TIMESTAMP
WHERE NOT EXISTS (SELECT 1 FROM orleans_schema_history WHERE component = 'transactions' AND version = 1);
