IF OBJECT_ID(N'orleans_reminder', N'U') IS NULL
CREATE TABLE orleans_reminder (
    key_hash binary(16) NOT NULL,
    reminder_identity varbinary(max) NOT NULL,
    service_hash binary(16) NOT NULL,
    grain_key_hash binary(16) NOT NULL,
    grain_hash bigint NOT NULL,
    service_id nvarchar(max) NOT NULL,
    grain_id nvarchar(max) NOT NULL,
    reminder_name nvarchar(max) NOT NULL,
    start_at_ticks bigint NOT NULL,
    period_ticks bigint NOT NULL,
    etag binary(16) NOT NULL,
    CONSTRAINT pk_orleans_reminder PRIMARY KEY (key_hash),
    INDEX ix_orleans_reminder_range (service_hash, grain_hash),
    INDEX ix_orleans_reminder_grain (grain_key_hash));

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
SELECT N'reminders', 1, N'SqlServer-0001-reminders.sql', NULL, N'manual', CURRENT_TIMESTAMP
WHERE NOT EXISTS (SELECT 1 FROM orleans_schema_history WHERE component = N'reminders' AND version = 1);
