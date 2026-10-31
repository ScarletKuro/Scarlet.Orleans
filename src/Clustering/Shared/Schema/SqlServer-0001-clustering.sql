IF OBJECT_ID(N'orleans_cluster_version', N'U') IS NULL
CREATE TABLE orleans_cluster_version (
    cluster_hash binary(16) NOT NULL,
    cluster_identity varbinary(max) NOT NULL,
    cluster_id nvarchar(max) NOT NULL,
    version int NOT NULL,
    etag binary(16) NOT NULL,
    CONSTRAINT pk_orleans_cluster_version PRIMARY KEY (cluster_hash));

IF OBJECT_ID(N'orleans_cluster_member', N'U') IS NULL
CREATE TABLE orleans_cluster_member (
    cluster_hash binary(16) NOT NULL,
    key_hash binary(16) NOT NULL,
    member_identity varbinary(max) NOT NULL,
    silo_address nvarchar(max) NOT NULL,
    status int NOT NULL,
    proxy_port int NOT NULL,
    host_name nvarchar(max) NULL,
    silo_name nvarchar(max) NULL,
    role_name nvarchar(max) NULL,
    update_zone int NOT NULL,
    fault_zone int NOT NULL,
    suspect_times nvarchar(max) NULL,
    latest_suspect_ticks bigint NULL,
    start_ticks bigint NOT NULL,
    i_am_alive_ticks bigint NOT NULL,
    etag binary(16) NOT NULL,
    CONSTRAINT pk_orleans_cluster_member PRIMARY KEY (cluster_hash, key_hash));

-- Records this script in the schema history, so a database set up by hand passes the provider's startup validation.
IF OBJECT_ID(N'orleans_schema_history', N'U') IS NULL
CREATE TABLE orleans_schema_history (
    component nvarchar(128) NOT NULL,
    version int NOT NULL,
    script nvarchar(256) NOT NULL,
    checksum nvarchar(64) NULL,
    source nvarchar(16) NOT NULL,
    applied_on datetime2 NOT NULL,
    CONSTRAINT pk_orleans_schema_history PRIMARY KEY (component, version));
INSERT INTO orleans_schema_history (component, version, script, checksum, source, applied_on)
SELECT N'clustering', 1, N'SqlServer-0001-clustering.sql', NULL, N'manual', CURRENT_TIMESTAMP
WHERE NOT EXISTS (SELECT 1 FROM orleans_schema_history WHERE component = N'clustering' AND version = 1);
