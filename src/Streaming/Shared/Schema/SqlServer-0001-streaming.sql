-- Times that queries compare (visible_on, expires_on, remove_on, evict_on) are UTC ticks, which compare exactly on every
-- engine and driver; created_on and dead_on are only read by people.
IF OBJECT_ID(N'orleans_stream_queue', N'U') IS NULL
CREATE TABLE orleans_stream_queue (
    queue_hash binary(16) NOT NULL,
    queue_identity varbinary(max) NOT NULL,
    service_id nvarchar(max) NOT NULL,
    provider_name nvarchar(max) NOT NULL,
    queue_id nvarchar(max) NOT NULL,
    evict_on bigint NOT NULL,
    created_on datetime2 NOT NULL,
    CONSTRAINT pk_orleans_stream_queue PRIMARY KEY (queue_hash));

IF OBJECT_ID(N'orleans_stream_message', N'U') IS NULL
CREATE TABLE orleans_stream_message (
    queue_hash binary(16) NOT NULL,
    message_id bigint IDENTITY(1, 1) NOT NULL,
    message_key binary(16) NOT NULL,
    dequeued int NOT NULL,
    visible_on bigint NOT NULL,
    expires_on bigint NOT NULL,
    created_on datetime2 NOT NULL,
    payload varbinary(max) NOT NULL,
    CONSTRAINT pk_orleans_stream_message PRIMARY KEY (message_id));

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'ix_orleans_stream_message_queue' AND object_id = OBJECT_ID(N'orleans_stream_message'))
CREATE INDEX ix_orleans_stream_message_queue ON orleans_stream_message (queue_hash, message_id, dequeued, visible_on, expires_on);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'ix_orleans_stream_message_key' AND object_id = OBJECT_ID(N'orleans_stream_message'))
CREATE UNIQUE INDEX ix_orleans_stream_message_key ON orleans_stream_message (message_key);

IF OBJECT_ID(N'orleans_stream_dead_letter', N'U') IS NULL
CREATE TABLE orleans_stream_dead_letter (
    queue_hash binary(16) NOT NULL,
    message_id bigint NOT NULL,
    message_key binary(16) NOT NULL,
    dequeued int NOT NULL,
    expires_on bigint NOT NULL,
    created_on datetime2 NOT NULL,
    payload varbinary(max) NOT NULL,
    dead_on datetime2 NOT NULL,
    remove_on bigint NOT NULL,
    CONSTRAINT pk_orleans_stream_dead_letter PRIMARY KEY (queue_hash, message_id));

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'ix_orleans_stream_dead_letter_remove_on' AND object_id = OBJECT_ID(N'orleans_stream_dead_letter'))
CREATE INDEX ix_orleans_stream_dead_letter_remove_on ON orleans_stream_dead_letter (queue_hash, remove_on);

-- Records this script in the schema history, so a database set up by hand passes the provider's startup validation.
IF OBJECT_ID(N'orleans_schema_history', N'U') IS NULL
CREATE TABLE orleans_schema_history (
    component nvarchar(128) NOT NULL,
    version int NOT NULL,
    script nvarchar(256) NOT NULL,
    checksum nvarchar(64) NULL,
    source nvarchar(16) NOT NULL,
    applied_on datetime2 NOT NULL,
    CONSTRAINT pk_orleans_schema_history PRIMARY KEY (component, version));
INSERT INTO orleans_schema_history (component, version, script, checksum, source, applied_on)
SELECT N'streaming', 1, N'SqlServer-0001-streaming.sql', NULL, N'manual', CURRENT_TIMESTAMP
WHERE NOT EXISTS (SELECT 1 FROM orleans_schema_history WHERE component = N'streaming' AND version = 1);
