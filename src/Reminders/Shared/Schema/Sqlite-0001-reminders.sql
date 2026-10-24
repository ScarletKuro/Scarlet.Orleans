CREATE TABLE IF NOT EXISTS orleans_reminder (
    key_hash BLOB NOT NULL,
    reminder_identity BLOB NOT NULL,
    service_hash BLOB NOT NULL,
    grain_key_hash BLOB NOT NULL,
    grain_hash INTEGER NOT NULL,
    service_id TEXT NOT NULL,
    grain_id TEXT NOT NULL,
    reminder_name TEXT NOT NULL,
    start_at_ticks INTEGER NOT NULL,
    period_ticks INTEGER NOT NULL,
    etag BLOB NOT NULL,
    CONSTRAINT pk_orleans_reminder PRIMARY KEY (key_hash));
CREATE INDEX IF NOT EXISTS ix_orleans_reminder_range ON orleans_reminder (service_hash, grain_hash);
CREATE INDEX IF NOT EXISTS ix_orleans_reminder_grain ON orleans_reminder (grain_key_hash);

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
SELECT 'reminders', 1, 'Sqlite-0001-reminders.sql', NULL, 'manual', CURRENT_TIMESTAMP
WHERE NOT EXISTS (SELECT 1 FROM orleans_schema_history WHERE component = 'reminders' AND version = 1);
