-- LakeWire coordinator schema v1. PRD §8, plus A2 (topic soft delete) and A8 (cluster id).
-- No consumer-group tables (v0.2). synchronous_commit stays at the server default.

CREATE TABLE topics (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    partitions INT NOT NULL,
    config JSONB NOT NULL DEFAULT '{}',
    tenant TEXT NOT NULL DEFAULT 'default',
    created_at TIMESTAMPTZ DEFAULT now(),
    deleted_at TIMESTAMPTZ NULL
);

-- A2: a deleted name can be reused. Active names stay unique.
CREATE UNIQUE INDEX topics_name_active_idx ON topics (name) WHERE deleted_at IS NULL;

CREATE TABLE partitions (
    topic_id BIGINT REFERENCES topics,
    part INT,
    next_offset BIGINT NOT NULL DEFAULT 0,
    log_start_offset BIGINT NOT NULL DEFAULT 0,
    coord_shard SMALLINT NOT NULL DEFAULT 0,
    PRIMARY KEY (topic_id, part)
);

CREATE TABLE brokers (
    id TEXT PRIMARY KEY,
    zone TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    epoch BIGINT NOT NULL,
    lease_expires_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE upload_intents (
    object_key TEXT PRIMARY KEY,
    broker_id TEXT NOT NULL,
    broker_epoch BIGINT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE objects (
    id UUID PRIMARY KEY,
    key TEXT UNIQUE NOT NULL,
    size_bytes BIGINT NOT NULL,
    broker_id TEXT,
    broker_epoch BIGINT,
    live_refs INT NOT NULL,
    live_bytes BIGINT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('COMMITTED', 'DELETE_PENDING')),
    delete_after TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE segments (
    topic_id BIGINT,
    part INT,
    base_offset BIGINT,
    last_offset BIGINT,
    object_id UUID NOT NULL REFERENCES objects,
    byte_start BIGINT,
    byte_len INT,
    n_batches SMALLINT,
    max_ts BIGINT,
    PRIMARY KEY (topic_id, part, base_offset)
);

CREATE INDEX ON segments (object_id);

CREATE TABLE producer_ids (
    producer_id BIGSERIAL PRIMARY KEY,
    producer_epoch SMALLINT NOT NULL DEFAULT 0,
    last_used TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE producer_state (
    producer_id BIGINT,
    topic_id BIGINT,
    part INT,
    producer_epoch SMALLINT NOT NULL,
    last_seq INT NOT NULL,
    recent JSONB NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (producer_id, topic_id, part)
);

-- A8: one stable Kafka-style cluster id (base64url, no padding).
CREATE TABLE cluster (
    id INT PRIMARY KEY CHECK (id = 1),
    cluster_id TEXT NOT NULL
);

INSERT INTO cluster (id, cluster_id)
VALUES (
    1,
    rtrim(translate(encode(uuid_send(gen_random_uuid()), 'base64'), '+/', '-_'), '=')
);
