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
