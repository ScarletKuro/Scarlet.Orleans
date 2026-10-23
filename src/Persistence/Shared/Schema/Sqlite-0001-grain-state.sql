CREATE TABLE IF NOT EXISTS orleans_grain_state (
    key_hash BLOB NOT NULL,
    grain_identity BLOB NOT NULL,
    service_id TEXT NOT NULL,
    state_name TEXT NOT NULL,
    grain_id TEXT NOT NULL,
    payload BLOB NULL,
    etag BLOB NOT NULL,
    modified_on TEXT NOT NULL,
    CONSTRAINT pk_orleans_grain_state PRIMARY KEY (key_hash));
