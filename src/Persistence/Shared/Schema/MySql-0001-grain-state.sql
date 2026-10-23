CREATE TABLE IF NOT EXISTS orleans_grain_state (
    key_hash binary(16) NOT NULL,
    grain_identity longblob NOT NULL,
    service_id longtext NOT NULL,
    state_name longtext NOT NULL,
    grain_id longtext NOT NULL,
    payload longblob NULL,
    etag binary(16) NOT NULL,
    modified_on datetime(6) NOT NULL,
    CONSTRAINT pk_orleans_grain_state PRIMARY KEY (key_hash)) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
