CREATE TABLE IF NOT EXISTS orleans_cluster_version (
    cluster_hash BLOB NOT NULL,
    cluster_identity BLOB NOT NULL,
    cluster_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    etag BLOB NOT NULL,
    CONSTRAINT pk_orleans_cluster_version PRIMARY KEY (cluster_hash));

CREATE TABLE IF NOT EXISTS orleans_cluster_member (
    cluster_hash BLOB NOT NULL,
    key_hash BLOB NOT NULL,
    member_identity BLOB NOT NULL,
    silo_address TEXT NOT NULL,
    status INTEGER NOT NULL,
    proxy_port INTEGER NOT NULL,
    host_name TEXT NULL,
    silo_name TEXT NULL,
    role_name TEXT NULL,
    update_zone INTEGER NOT NULL,
    fault_zone INTEGER NOT NULL,
    suspect_times TEXT NULL,
    latest_suspect_ticks INTEGER NULL,
    start_ticks INTEGER NOT NULL,
    i_am_alive_ticks INTEGER NOT NULL,
    etag BLOB NOT NULL,
    CONSTRAINT pk_orleans_cluster_member PRIMARY KEY (cluster_hash, key_hash));

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
SELECT 'clustering', 1, 'Sqlite-0001-clustering.sql', NULL, 'manual', CURRENT_TIMESTAMP
WHERE NOT EXISTS (SELECT 1 FROM orleans_schema_history WHERE component = 'clustering' AND version = 1);
