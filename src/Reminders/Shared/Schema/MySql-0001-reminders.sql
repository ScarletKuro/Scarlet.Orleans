CREATE TABLE IF NOT EXISTS orleans_reminder (
    key_hash binary(16) NOT NULL,
    reminder_identity longblob NOT NULL,
    service_hash binary(16) NOT NULL,
    grain_key_hash binary(16) NOT NULL,
    grain_hash bigint NOT NULL,
    service_id longtext NOT NULL,
    grain_id longtext NOT NULL,
    reminder_name longtext NOT NULL,
    start_at_ticks bigint NOT NULL,
    period_ticks bigint NOT NULL,
    etag binary(16) NOT NULL,
    CONSTRAINT pk_orleans_reminder PRIMARY KEY (key_hash),
    INDEX ix_orleans_reminder_range (service_hash, grain_hash),
    INDEX ix_orleans_reminder_grain (grain_key_hash)) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

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
SELECT 'reminders', 1, 'MySql-0001-reminders.sql', NULL, 'manual', CURRENT_TIMESTAMP FROM DUAL
WHERE NOT EXISTS (SELECT 1 FROM orleans_schema_history WHERE component = 'reminders' AND version = 1);
