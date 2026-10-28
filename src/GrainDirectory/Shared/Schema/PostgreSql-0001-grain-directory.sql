CREATE TABLE IF NOT EXISTS orleans_grain_directory (
    key_hash bytea NOT NULL,
    grain_identity bytea NOT NULL,
    scope_hash bytea NOT NULL,
    silo_hash bytea NOT NULL,
    cluster_id text NOT NULL,
    directory_name text NOT NULL,
    grain_id text NOT NULL,
    silo_address text NOT NULL,
    activation_id bytea NOT NULL,
    membership_version bigint NOT NULL,
    CONSTRAINT pk_orleans_grain_directory PRIMARY KEY (key_hash));
CREATE INDEX IF NOT EXISTS ix_orleans_grain_directory_silo ON orleans_grain_directory (scope_hash, silo_hash, key_hash);

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
SELECT 'grain-directory', 1, 'PostgreSql-0001-grain-directory.sql', NULL, 'manual', CURRENT_TIMESTAMP
WHERE NOT EXISTS (SELECT 1 FROM orleans_schema_history WHERE component = 'grain-directory' AND version = 1);
