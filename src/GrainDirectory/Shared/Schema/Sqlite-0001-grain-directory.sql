CREATE TABLE IF NOT EXISTS orleans_grain_directory (
    key_hash BLOB NOT NULL,
    grain_identity BLOB NOT NULL,
    scope_hash BLOB NOT NULL,
    silo_hash BLOB NOT NULL,
    cluster_id TEXT NOT NULL,
    directory_name TEXT NOT NULL,
    grain_id TEXT NOT NULL,
    silo_address TEXT NOT NULL,
    activation_id BLOB NOT NULL,
    membership_version INTEGER NOT NULL,
    CONSTRAINT pk_orleans_grain_directory PRIMARY KEY (key_hash));
CREATE INDEX IF NOT EXISTS ix_orleans_grain_directory_silo ON orleans_grain_directory (scope_hash, silo_hash, key_hash);

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
SELECT 'grain-directory', 1, 'Sqlite-0001-grain-directory.sql', NULL, 'manual', CURRENT_TIMESTAMP
WHERE NOT EXISTS (SELECT 1 FROM orleans_schema_history WHERE component = 'grain-directory' AND version = 1);
