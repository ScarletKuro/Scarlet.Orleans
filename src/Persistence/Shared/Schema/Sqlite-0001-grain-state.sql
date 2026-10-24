CREATE TABLE IF NOT EXISTS orleans_grain_state (
    key_hash BLOB NOT NULL,
    grain_identity BLOB NOT NULL,
    service_id TEXT NOT NULL,
    state_name TEXT NOT NULL,
    grain_id TEXT NOT NULL,
    payload BLOB NULL,
    etag BLOB NOT NULL,
    modified_on TEXT NOT NULL,
    CONSTRAINT pk_orleans_grain_state PRIMARY KEY (key_hash));

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
SELECT 'persistence', 1, 'Sqlite-0001-grain-state.sql', NULL, 'manual', CURRENT_TIMESTAMP
WHERE NOT EXISTS (SELECT 1 FROM orleans_schema_history WHERE component = 'persistence' AND version = 1);
