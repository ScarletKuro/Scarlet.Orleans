IF OBJECT_ID(N'orleans_grain_directory', N'U') IS NULL
CREATE TABLE orleans_grain_directory (
    key_hash binary(16) NOT NULL,
    grain_identity varbinary(max) NOT NULL,
    scope_hash binary(16) NOT NULL,
    silo_hash binary(16) NOT NULL,
    cluster_id nvarchar(max) NOT NULL,
    directory_name nvarchar(max) NOT NULL,
    grain_id nvarchar(max) NOT NULL,
    silo_address nvarchar(max) NOT NULL,
    activation_id binary(16) NOT NULL,
    membership_version bigint NOT NULL,
    CONSTRAINT pk_orleans_grain_directory PRIMARY KEY (key_hash),
    INDEX ix_orleans_grain_directory_silo (scope_hash, silo_hash, key_hash));

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
SELECT N'grain-directory', 1, N'SqlServer-0001-grain-directory.sql', NULL, N'manual', CURRENT_TIMESTAMP
WHERE NOT EXISTS (SELECT 1 FROM orleans_schema_history WHERE component = N'grain-directory' AND version = 1);
