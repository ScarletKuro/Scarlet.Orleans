CREATE TABLE IF NOT EXISTS orleans_grain_state (
    key_hash bytea NOT NULL,
    grain_identity bytea NOT NULL,
    service_id text NOT NULL,
    state_name text NOT NULL,
    grain_id text NOT NULL,
    payload bytea NULL,
    etag bytea NOT NULL,
    modified_on timestamp with time zone NOT NULL,
    CONSTRAINT pk_orleans_grain_state PRIMARY KEY (key_hash));

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
SELECT 'persistence', 1, 'PostgreSql-0001-grain-state.sql', NULL, 'manual', CURRENT_TIMESTAMP
WHERE NOT EXISTS (SELECT 1 FROM orleans_schema_history WHERE component = 'persistence' AND version = 1);
