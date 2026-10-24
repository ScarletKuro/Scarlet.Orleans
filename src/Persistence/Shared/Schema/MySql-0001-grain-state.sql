CREATE TABLE IF NOT EXISTS orleans_grain_state (
    key_hash binary(16) NOT NULL,
    grain_identity longblob NOT NULL,
    service_id longtext NOT NULL,
    state_name longtext NOT NULL,
    grain_id longtext NOT NULL,
    payload longblob NULL,
    etag binary(16) NOT NULL,
    modified_on datetime(6) NOT NULL,
    CONSTRAINT pk_orleans_grain_state PRIMARY KEY (key_hash)) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

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
SELECT 'persistence', 1, 'MySql-0001-grain-state.sql', NULL, 'manual', CURRENT_TIMESTAMP FROM DUAL
WHERE NOT EXISTS (SELECT 1 FROM orleans_schema_history WHERE component = 'persistence' AND version = 1);
