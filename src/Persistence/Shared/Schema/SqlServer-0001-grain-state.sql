IF OBJECT_ID(N'orleans_grain_state', N'U') IS NULL
CREATE TABLE orleans_grain_state (
    key_hash binary(16) NOT NULL,
    grain_identity varbinary(max) NOT NULL,
    service_id nvarchar(max) NOT NULL,
    state_name nvarchar(max) NOT NULL,
    grain_id nvarchar(max) NOT NULL,
    payload varbinary(max) NULL,
    etag binary(16) NOT NULL,
    modified_on datetime2 NOT NULL,
    CONSTRAINT pk_orleans_grain_state PRIMARY KEY (key_hash));
