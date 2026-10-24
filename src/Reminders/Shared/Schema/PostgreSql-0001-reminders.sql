CREATE TABLE IF NOT EXISTS orleans_reminder (
    key_hash bytea NOT NULL,
    reminder_identity bytea NOT NULL,
    service_hash bytea NOT NULL,
    grain_key_hash bytea NOT NULL,
    grain_hash bigint NOT NULL,
    service_id text NOT NULL,
    grain_id text NOT NULL,
    reminder_name text NOT NULL,
    start_at_ticks bigint NOT NULL,
    period_ticks bigint NOT NULL,
    etag bytea NOT NULL,
    CONSTRAINT pk_orleans_reminder PRIMARY KEY (key_hash));
CREATE INDEX IF NOT EXISTS ix_orleans_reminder_range ON orleans_reminder (service_hash, grain_hash);
CREATE INDEX IF NOT EXISTS ix_orleans_reminder_grain ON orleans_reminder (grain_key_hash);

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
SELECT 'reminders', 1, 'PostgreSql-0001-reminders.sql', NULL, 'manual', CURRENT_TIMESTAMP
WHERE NOT EXISTS (SELECT 1 FROM orleans_schema_history WHERE component = 'reminders' AND version = 1);
