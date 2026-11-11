-- Times that queries compare (visible_on, expires_on, remove_on, evict_on) are UTC ticks, which compare exactly on every
-- engine and driver; created_on and dead_on are only read by people. InnoDB requires an auto-increment column to lead an
-- index, hence ix_orleans_stream_message_id.
CREATE TABLE IF NOT EXISTS orleans_stream_queue (
    queue_hash binary(16) NOT NULL,
    queue_identity longblob NOT NULL,
    service_id longtext NOT NULL,
    provider_name longtext NOT NULL,
    queue_id longtext NOT NULL,
    evict_on bigint NOT NULL,
    created_on datetime(6) NOT NULL,
    CONSTRAINT pk_orleans_stream_queue PRIMARY KEY (queue_hash)) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE TABLE IF NOT EXISTS orleans_stream_message (
    queue_hash binary(16) NOT NULL,
    message_id bigint NOT NULL AUTO_INCREMENT,
    message_key binary(16) NOT NULL,
    dequeued int NOT NULL,
    visible_on bigint NOT NULL,
    expires_on bigint NOT NULL,
    created_on datetime(6) NOT NULL,
    payload longblob NOT NULL,
    CONSTRAINT pk_orleans_stream_message PRIMARY KEY (queue_hash, message_id),
    CONSTRAINT uq_orleans_stream_message_key UNIQUE (message_key),
    INDEX ix_orleans_stream_message_id (message_id)) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE TABLE IF NOT EXISTS orleans_stream_dead_letter (
    queue_hash binary(16) NOT NULL,
    message_id bigint NOT NULL,
    message_key binary(16) NOT NULL,
    dequeued int NOT NULL,
    expires_on bigint NOT NULL,
    created_on datetime(6) NOT NULL,
    payload longblob NOT NULL,
    dead_on datetime(6) NOT NULL,
    remove_on bigint NOT NULL,
    CONSTRAINT pk_orleans_stream_dead_letter PRIMARY KEY (queue_hash, message_id),
    INDEX ix_orleans_stream_dead_letter_remove_on (queue_hash, remove_on)) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

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
SELECT 'streaming', 1, 'MySql-0001-streaming.sql', NULL, 'manual', CURRENT_TIMESTAMP FROM DUAL
WHERE NOT EXISTS (SELECT 1 FROM orleans_schema_history WHERE component = 'streaming' AND version = 1);
