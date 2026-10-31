CREATE TABLE IF NOT EXISTS orleans_transactional_state (
    key_hash binary(16) NOT NULL,
    state_identity longblob NOT NULL,
    service_id longtext NOT NULL,
    state_name longtext NOT NULL,
    grain_id longtext NOT NULL,
    etag binary(16) NOT NULL,
    committed_sequence_id bigint NOT NULL,
    metadata longblob NOT NULL,
    modified_on datetime(6) NOT NULL,
    CONSTRAINT pk_orleans_transactional_state PRIMARY KEY (key_hash)) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE TABLE IF NOT EXISTS orleans_transactional_state_entry (
    key_hash binary(16) NOT NULL,
    sequence_id bigint NOT NULL,
    transaction_id longtext NOT NULL,
    timestamp_ticks bigint NOT NULL,
    transaction_manager longblob NOT NULL,
    payload longblob NOT NULL,
    CONSTRAINT pk_orleans_transactional_state_entry PRIMARY KEY (key_hash, sequence_id)) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

-- Records this script in the schema history, so a database set up by hand passes the provider's startup validation.
CREATE TABLE IF NOT EXISTS orleans_schema_history (
    component varchar(128) NOT NULL,
    version int NOT NULL,
    script varchar(256) NOT NULL,
    checksum varchar(64) NULL,
    source varchar(16) NOT NULL,
    applied_on datetime(6) NOT NULL,
    CONSTRAINT pk_orleans_schema_history PRIMARY KEY (component, version)) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
INSERT INTO orleans_schema_history (component, version, script, checksum, source, applied_on)
SELECT 'transactions', 1, 'MySql-0001-transactions.sql', NULL, 'manual', CURRENT_TIMESTAMP FROM DUAL
WHERE NOT EXISTS (SELECT 1 FROM orleans_schema_history WHERE component = 'transactions' AND version = 1);
