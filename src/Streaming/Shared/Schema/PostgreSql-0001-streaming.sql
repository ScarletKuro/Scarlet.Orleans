-- Times that queries compare (visible_on, expires_on, remove_on, evict_on) are UTC ticks, which compare exactly on every
-- engine and driver; created_on and dead_on are only read by people.
CREATE TABLE IF NOT EXISTS orleans_stream_queue (
    queue_hash bytea NOT NULL,
    queue_identity bytea NOT NULL,
    service_id text NOT NULL,
    provider_name text NOT NULL,
    queue_id text NOT NULL,
    evict_on bigint NOT NULL,
    created_on timestamp with time zone NOT NULL,
    CONSTRAINT pk_orleans_stream_queue PRIMARY KEY (queue_hash));

CREATE TABLE IF NOT EXISTS orleans_stream_message (
    queue_hash bytea NOT NULL,
    message_id bigint GENERATED ALWAYS AS IDENTITY,
    message_key bytea NOT NULL,
    dequeued integer NOT NULL,
    visible_on bigint NOT NULL,
    expires_on bigint NOT NULL,
    created_on timestamp with time zone NOT NULL,
    payload bytea NOT NULL,
    CONSTRAINT pk_orleans_stream_message PRIMARY KEY (queue_hash, message_id),
    CONSTRAINT uq_orleans_stream_message_key UNIQUE (message_key));

CREATE TABLE IF NOT EXISTS orleans_stream_dead_letter (
    queue_hash bytea NOT NULL,
    message_id bigint NOT NULL,
    message_key bytea NOT NULL,
    dequeued integer NOT NULL,
    expires_on bigint NOT NULL,
    created_on timestamp with time zone NOT NULL,
    payload bytea NOT NULL,
    dead_on timestamp with time zone NOT NULL,
    remove_on bigint NOT NULL,
    CONSTRAINT pk_orleans_stream_dead_letter PRIMARY KEY (queue_hash, message_id));

CREATE INDEX IF NOT EXISTS ix_orleans_stream_dead_letter_remove_on ON orleans_stream_dead_letter (queue_hash, remove_on);

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
SELECT 'streaming', 1, 'PostgreSql-0001-streaming.sql', NULL, 'manual', CURRENT_TIMESTAMP
WHERE NOT EXISTS (SELECT 1 FROM orleans_schema_history WHERE component = 'streaming' AND version = 1);
