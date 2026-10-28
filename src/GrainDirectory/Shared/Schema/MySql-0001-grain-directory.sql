CREATE TABLE IF NOT EXISTS orleans_grain_directory (
    key_hash binary(16) NOT NULL,
    grain_identity longblob NOT NULL,
    scope_hash binary(16) NOT NULL,
    silo_hash binary(16) NOT NULL,
    cluster_id longtext NOT NULL,
    directory_name longtext NOT NULL,
    grain_id longtext NOT NULL,
    silo_address longtext NOT NULL,
    activation_id binary(16) NOT NULL,
    membership_version bigint NOT NULL,
    CONSTRAINT pk_orleans_grain_directory PRIMARY KEY (key_hash),
    INDEX ix_orleans_grain_directory_silo (scope_hash, silo_hash, key_hash)) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

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
SELECT 'grain-directory', 1, 'MySql-0001-grain-directory.sql', NULL, 'manual', CURRENT_TIMESTAMP FROM DUAL
WHERE NOT EXISTS (SELECT 1 FROM orleans_schema_history WHERE component = 'grain-directory' AND version = 1);
