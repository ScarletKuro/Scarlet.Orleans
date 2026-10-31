CREATE TABLE IF NOT EXISTS orleans_cluster_version (
    cluster_hash bytea NOT NULL,
    cluster_identity bytea NOT NULL,
    cluster_id text NOT NULL,
    version integer NOT NULL,
    etag bytea NOT NULL,
    CONSTRAINT pk_orleans_cluster_version PRIMARY KEY (cluster_hash));

CREATE TABLE IF NOT EXISTS orleans_cluster_member (
    cluster_hash bytea NOT NULL,
    key_hash bytea NOT NULL,
    member_identity bytea NOT NULL,
    silo_address text NOT NULL,
    status integer NOT NULL,
    proxy_port integer NOT NULL,
    host_name text NULL,
    silo_name text NULL,
    role_name text NULL,
    update_zone integer NOT NULL,
    fault_zone integer NOT NULL,
    suspect_times text NULL,
    latest_suspect_ticks bigint NULL,
    start_ticks bigint NOT NULL,
    i_am_alive_ticks bigint NOT NULL,
    etag bytea NOT NULL,
    CONSTRAINT pk_orleans_cluster_member PRIMARY KEY (cluster_hash, key_hash));

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
SELECT 'clustering', 1, 'PostgreSql-0001-clustering.sql', NULL, 'manual', CURRENT_TIMESTAMP
WHERE NOT EXISTS (SELECT 1 FROM orleans_schema_history WHERE component = 'clustering' AND version = 1);
