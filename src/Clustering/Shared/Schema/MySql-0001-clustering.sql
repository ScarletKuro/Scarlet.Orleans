CREATE TABLE IF NOT EXISTS orleans_cluster_version (
    cluster_hash binary(16) NOT NULL,
    cluster_identity longblob NOT NULL,
    cluster_id longtext NOT NULL,
    version int NOT NULL,
    etag binary(16) NOT NULL,
    CONSTRAINT pk_orleans_cluster_version PRIMARY KEY (cluster_hash)) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE TABLE IF NOT EXISTS orleans_cluster_member (
    cluster_hash binary(16) NOT NULL,
    key_hash binary(16) NOT NULL,
    member_identity longblob NOT NULL,
    silo_address longtext NOT NULL,
    status int NOT NULL,
    proxy_port int NOT NULL,
    host_name longtext NULL,
    silo_name longtext NULL,
    role_name longtext NULL,
    update_zone int NOT NULL,
    fault_zone int NOT NULL,
    suspect_times longtext NULL,
    latest_suspect_ticks bigint NULL,
    start_ticks bigint NOT NULL,
    i_am_alive_ticks bigint NOT NULL,
    etag binary(16) NOT NULL,
    CONSTRAINT pk_orleans_cluster_member PRIMARY KEY (cluster_hash, key_hash)) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

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
SELECT 'clustering', 1, 'MySql-0001-clustering.sql', NULL, 'manual', CURRENT_TIMESTAMP FROM DUAL
WHERE NOT EXISTS (SELECT 1 FROM orleans_schema_history WHERE component = 'clustering' AND version = 1);
