-- Coordinator functions. Repeatable so M6 can replace sl_check_sequence in place.
-- Production commits call sl_commit_object (set-based). sl_commit_object_rowloop is the Day-3 lab only.

CREATE OR REPLACE FUNCTION sl_kafka_error_codes()
RETURNS TABLE (name text, code smallint)
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT * FROM (VALUES
        ('UNKNOWN_TOPIC_OR_PARTITION'::text, 3::smallint),
        ('NOT_LEADER_OR_FOLLOWER'::text, 6::smallint),
        ('REQUEST_TIMED_OUT'::text, 7::smallint),
        ('INVALID_TOPIC_EXCEPTION'::text, 17::smallint),
        ('TOPIC_ALREADY_EXISTS'::text, 36::smallint),
        ('INVALID_PARTITIONS'::text, 37::smallint),
        ('INVALID_CONFIG'::text, 40::smallint),
        ('OUT_OF_ORDER_SEQUENCE_NUMBER'::text, 45::smallint),
        ('INVALID_PRODUCER_EPOCH'::text, 47::smallint),
        ('UNKNOWN_PRODUCER_ID'::text, 59::smallint)
    ) AS codes(name, code);
$$;

-- M1 stub. M6 replaces this body. Duplicate batches later return error 0 and a non-null dup base.
CREATE OR REPLACE FUNCTION sl_check_sequence(
    p_producer_id bigint,
    p_producer_epoch smallint,
    p_topic_id bigint,
    p_part int,
    p_first_seq int,
    p_last_seq int
) RETURNS TABLE (error_code smallint, dup_base_offset bigint)
LANGUAGE plpgsql
AS $$
BEGIN
    error_code := 0;
    dup_base_offset := NULL;
    RETURN NEXT;
END;
$$;

CREATE OR REPLACE FUNCTION sl_register_broker(
    p_id text,
    p_zone text,
    p_endpoint text,
    p_lease_ms bigint DEFAULT 10000
) RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
    v_epoch bigint;
BEGIN
    INSERT INTO brokers (id, zone, endpoint, epoch, lease_expires_at)
    VALUES (
        p_id,
        p_zone,
        p_endpoint,
        1,
        now() + (p_lease_ms * interval '1 millisecond')
    )
    ON CONFLICT (id) DO UPDATE
        SET zone = EXCLUDED.zone,
            endpoint = EXCLUDED.endpoint,
            epoch = brokers.epoch + 1,
            lease_expires_at = now() + (p_lease_ms * interval '1 millisecond')
    RETURNING epoch INTO v_epoch;
    RETURN v_epoch;
END;
$$;

CREATE OR REPLACE FUNCTION sl_heartbeat(
    p_id text,
    p_epoch bigint,
    p_lease_ms bigint DEFAULT 10000
) RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    v_updated int;
BEGIN
    UPDATE brokers
    SET lease_expires_at = now() + (p_lease_ms * interval '1 millisecond')
    WHERE id = p_id
      AND epoch = p_epoch;
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    RETURN v_updated = 1;
END;
$$;

-- ON CONFLICT DO NOTHING: a fenced broker must not steal an existing intent.
CREATE OR REPLACE FUNCTION sl_create_upload_intent(
    p_key text,
    p_broker_id text,
    p_broker_epoch bigint
) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO upload_intents (object_key, broker_id, broker_epoch)
    VALUES (p_key, p_broker_id, p_broker_epoch)
    ON CONFLICT (object_key) DO NOTHING;
END;
$$;

CREATE OR REPLACE FUNCTION sl_commit_object_impl(
    p_object_id uuid,
    p_key text,
    p_size bigint,
    p_broker_id text,
    p_broker_epoch bigint,
    p_topic_ids bigint[],
    p_parts int[],
    p_byte_starts bigint[],
    p_byte_lens int[],
    p_n_records int[],
    p_n_batches smallint[],
    p_max_ts bigint[],
    p_producer_ids bigint[],
    p_producer_epochs smallint[],
    p_first_seqs int[],
    p_last_seqs int[],
    p_row_loop boolean
) RETURNS TABLE (idx int, base_offset bigint, error_code smallint, log_start_offset bigint)
LANGUAGE plpgsql
AS $$
DECLARE
    v_n int;
    v_not_leader smallint;
    v_unknown smallint;
    v_object_id uuid;
    v_seq_err smallint;
    v_dup bigint;
    v_start bigint;
    v_lso bigint;
    v_refs int;
    v_live_bytes bigint;
    v_used text[] := ARRAY[]::text[];
    v_base bigint;
    v_tag text;
    r record;
    i int;
BEGIN
    v_not_leader := (
        SELECT c.code FROM sl_kafka_error_codes() c WHERE c.name = 'NOT_LEADER_OR_FOLLOWER'
    );
    v_unknown := (
        SELECT c.code FROM sl_kafka_error_codes() c WHERE c.name = 'UNKNOWN_TOPIC_OR_PARTITION'
    );
    IF v_not_leader IS NULL OR v_unknown IS NULL THEN
        RAISE EXCEPTION 'sl_kafka_error_codes is missing a required code';
    END IF;

    v_n := coalesce(cardinality(p_topic_ids), 0);
    IF coalesce(cardinality(p_parts), -1) IS DISTINCT FROM v_n
        OR coalesce(cardinality(p_byte_starts), -1) IS DISTINCT FROM v_n
        OR coalesce(cardinality(p_byte_lens), -1) IS DISTINCT FROM v_n
        OR coalesce(cardinality(p_n_records), -1) IS DISTINCT FROM v_n
        OR coalesce(cardinality(p_n_batches), -1) IS DISTINCT FROM v_n
        OR coalesce(cardinality(p_max_ts), -1) IS DISTINCT FROM v_n
        OR coalesce(cardinality(p_producer_ids), -1) IS DISTINCT FROM v_n
        OR coalesce(cardinality(p_producer_epochs), -1) IS DISTINCT FROM v_n
        OR coalesce(cardinality(p_first_seqs), -1) IS DISTINCT FROM v_n
        OR coalesce(cardinality(p_last_seqs), -1) IS DISTINCT FROM v_n
    THEN
        RAISE EXCEPTION 'sl_commit_object array lengths differ';
    END IF;

    IF v_n > 0 AND EXISTS (SELECT 1 FROM unnest(p_n_records) AS n(n) WHERE n.n < 1) THEN
        RAISE EXCEPTION 'sl_commit_object n_records must be >= 1';
    END IF;

    -- 1. Epoch and lease, before replay, so a stale broker cannot read a stored commit either.
    IF NOT EXISTS (
        SELECT 1
        FROM brokers b
        WHERE b.id = p_broker_id
          AND b.epoch = p_broker_epoch
          AND b.lease_expires_at > now()
    ) THEN
        RETURN QUERY
        SELECT s.i - 1, -1::bigint, v_not_leader, -1::bigint
        FROM generate_subscripts(p_topic_ids, 1) AS s(i);
        RETURN;
    END IF;

    -- 2. Ambiguous-commit replay. Same key returns the stored offsets and writes nothing.
    SELECT o.id INTO v_object_id FROM objects o WHERE o.key = p_key;
    IF FOUND THEN
        FOR i IN 1..v_n LOOP
            v_base := NULL;
            v_lso := -1;
            IF NOT EXISTS (
                SELECT 1
                FROM partitions p
                JOIN topics t ON t.id = p.topic_id
                WHERE p.topic_id = p_topic_ids[i]
                  AND p.part = p_parts[i]
                  AND t.deleted_at IS NULL
            ) THEN
                SELECT p.log_start_offset INTO v_lso
                FROM partitions p
                WHERE p.topic_id = p_topic_ids[i]
                  AND p.part = p_parts[i];
                IF NOT FOUND THEN
                    v_lso := -1;
                END IF;
                RETURN QUERY SELECT i - 1, -1::bigint, v_unknown, v_lso;
            ELSE
                v_tag := p_topic_ids[i]::text || ':' || p_parts[i]::text;
                SELECT s.base_offset, p.log_start_offset
                INTO v_base, v_lso
                FROM segments s
                JOIN partitions p ON p.topic_id = s.topic_id AND p.part = s.part
                WHERE s.object_id = v_object_id
                  AND s.topic_id = p_topic_ids[i]
                  AND s.part = p_parts[i]
                  AND NOT ((v_tag || ':' || s.base_offset::text) = ANY (v_used))
                ORDER BY s.base_offset
                LIMIT 1;
                IF v_base IS NULL THEN
                    RAISE EXCEPTION
                        'sl_commit_object replay found no segment for topic % part %',
                        p_topic_ids[i], p_parts[i];
                END IF;
                v_used := v_used || (v_tag || ':' || v_base::text);
                RETURN QUERY SELECT i - 1, v_base, 0::smallint, v_lso;
            END IF;
        END LOOP;
        RETURN;
    END IF;

    -- 3. Intent. Missing row means GC or a fence won. Write nothing.
    DELETE FROM upload_intents
    WHERE object_key = p_key
      AND broker_id = p_broker_id
      AND broker_epoch = p_broker_epoch;
    IF NOT FOUND THEN
        RETURN QUERY
        SELECT s.i - 1, -1::bigint, v_not_leader, -1::bigint
        FROM generate_subscripts(p_topic_ids, 1) AS s(i);
        RETURN;
    END IF;

    -- Group commit calls this function more than once per transaction.
    -- ON COMMIT DROP removes the table at txn end; TRUNCATE clears the previous call.
    CREATE TEMP TABLE IF NOT EXISTS sl_commit_work (
        entry_idx int PRIMARY KEY,
        topic_id bigint NOT NULL,
        part int NOT NULL,
        byte_start bigint NOT NULL,
        byte_len int NOT NULL,
        n_records int NOT NULL,
        n_batches smallint NOT NULL,
        max_ts bigint NOT NULL,
        producer_id bigint NOT NULL,
        producer_epoch smallint NOT NULL,
        first_seq int NOT NULL,
        last_seq int NOT NULL,
        accepted boolean NOT NULL,
        is_dup boolean NOT NULL,
        err_code smallint NOT NULL,
        base_off bigint,
        lso bigint,
        hw_off bigint
    ) ON COMMIT DROP;
    TRUNCATE sl_commit_work;

    CREATE TEMP TABLE IF NOT EXISTS sl_commit_moved (
        topic_id bigint NOT NULL,
        part int NOT NULL,
        start_off bigint NOT NULL,
        lso bigint NOT NULL,
        hw_off bigint NOT NULL,
        PRIMARY KEY (topic_id, part)
    ) ON COMMIT DROP;
    TRUNCATE sl_commit_moved;

    INSERT INTO sl_commit_work (
        entry_idx, topic_id, part, byte_start, byte_len, n_records, n_batches, max_ts,
        producer_id, producer_epoch, first_seq, last_seq,
        accepted, is_dup, err_code
    )
    SELECT
        s.i - 1,
        p_topic_ids[s.i],
        p_parts[s.i],
        p_byte_starts[s.i],
        p_byte_lens[s.i],
        p_n_records[s.i],
        p_n_batches[s.i],
        p_max_ts[s.i],
        p_producer_ids[s.i],
        p_producer_epochs[s.i],
        p_first_seqs[s.i],
        p_last_seqs[s.i],
        true,
        false,
        0
    FROM generate_subscripts(p_topic_ids, 1) AS s(i);

    -- 4. Lock existing partition rows in (topic_id, part) order, then reject gaps and soft deletes.
    IF NOT p_row_loop THEN
        FOR r IN
            SELECT p.topic_id, p.part
            FROM partitions p
            WHERE (p.topic_id, p.part) IN (
                SELECT w.topic_id, w.part FROM sl_commit_work w
            )
            ORDER BY p.topic_id, p.part
            FOR UPDATE
        LOOP
            NULL;
        END LOOP;
    END IF;

    UPDATE sl_commit_work w
    SET accepted = false,
        err_code = v_unknown,
        base_off = -1,
        lso = p.log_start_offset
    FROM partitions p
    JOIN topics t ON t.id = p.topic_id
    WHERE w.topic_id = p.topic_id
      AND w.part = p.part
      AND t.deleted_at IS NOT NULL;

    UPDATE sl_commit_work w
    SET accepted = false,
        err_code = v_unknown,
        base_off = -1,
        lso = -1
    WHERE w.accepted
      AND NOT EXISTS (
          SELECT 1
          FROM partitions p
          WHERE p.topic_id = w.topic_id
            AND p.part = w.part
      );

    -- 5. Idempotence hook, in array order within each partition. producer_id < 0 skips it.
    FOR r IN
        SELECT w.entry_idx, w.topic_id, w.part, w.producer_id, w.producer_epoch, w.first_seq, w.last_seq
        FROM sl_commit_work w
        WHERE w.accepted
          AND w.producer_id >= 0
        ORDER BY w.topic_id, w.part, w.entry_idx
    LOOP
        SELECT s.error_code, s.dup_base_offset
        INTO v_seq_err, v_dup
        FROM sl_check_sequence(
            r.producer_id, r.producer_epoch, r.topic_id, r.part, r.first_seq, r.last_seq
        ) s;

        IF v_seq_err <> 0 THEN
            UPDATE sl_commit_work
            SET accepted = false,
                err_code = v_seq_err,
                base_off = -1,
                lso = COALESCE(
                    (SELECT p.log_start_offset FROM partitions p
                     WHERE p.topic_id = r.topic_id AND p.part = r.part),
                    -1
                )
            WHERE entry_idx = r.entry_idx;
        ELSIF v_dup IS NOT NULL THEN
            UPDATE sl_commit_work
            SET is_dup = true,
                err_code = 0,
                base_off = v_dup,
                lso = COALESCE(
                    (SELECT p.log_start_offset FROM partitions p
                     WHERE p.topic_id = r.topic_id AND p.part = r.part),
                    -1
                )
            WHERE entry_idx = r.entry_idx;
        END IF;
    END LOOP;

    -- 6. Offsets. The row-loop path is the lab baseline; production uses one set-based update.
    IF p_row_loop THEN
        FOR r IN
            SELECT w.entry_idx, w.topic_id, w.part, w.n_records
            FROM sl_commit_work w
            WHERE w.accepted
              AND NOT w.is_dup
            ORDER BY w.topic_id, w.part, w.entry_idx
        LOOP
            SELECT p.next_offset, p.log_start_offset
            INTO v_start, v_lso
            FROM partitions p
            WHERE p.topic_id = r.topic_id
              AND p.part = r.part
            FOR UPDATE;

            UPDATE partitions
            SET next_offset = v_start + r.n_records
            WHERE topic_id = r.topic_id
              AND part = r.part;

            UPDATE sl_commit_work
            SET base_off = v_start,
                lso = v_lso,
                hw_off = v_start + r.n_records
            WHERE entry_idx = r.entry_idx;
        END LOOP;
    ELSE
        WITH deltas AS (
            SELECT w.topic_id, w.part, sum(w.n_records)::bigint AS total
            FROM sl_commit_work w
            WHERE w.accepted
              AND NOT w.is_dup
            GROUP BY w.topic_id, w.part
        ),
        moved AS (
            UPDATE partitions p
            SET next_offset = p.next_offset + d.total
            FROM deltas d
            WHERE p.topic_id = d.topic_id
              AND p.part = d.part
            RETURNING p.topic_id, p.part,
                      (p.next_offset - d.total) AS start_off,
                      p.log_start_offset,
                      p.next_offset AS hw_off
        )
        INSERT INTO sl_commit_moved (topic_id, part, start_off, lso, hw_off)
        SELECT m.topic_id, m.part, m.start_off, m.log_start_offset, m.hw_off
        FROM moved m;

        UPDATE sl_commit_work w
        SET base_off = a.assigned,
            lso = a.lso,
            hw_off = a.hw_off
        FROM (
            SELECT
                w2.entry_idx,
                m.start_off + COALESCE(sum(w2.n_records) OVER (
                    PARTITION BY w2.topic_id, w2.part
                    ORDER BY w2.entry_idx
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ), 0) AS assigned,
                m.lso,
                m.hw_off
            FROM sl_commit_work w2
            JOIN sl_commit_moved m ON m.topic_id = w2.topic_id AND m.part = w2.part
            WHERE w2.accepted
              AND NOT w2.is_dup
        ) a
        WHERE w.entry_idx = a.entry_idx;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM sl_commit_work w
        WHERE w.accepted
          AND NOT w.is_dup
          AND w.base_off IS NULL
    ) THEN
        RAISE EXCEPTION 'sl_commit_object failed to assign offsets';
    END IF;

    SELECT count(*)::int, COALESCE(sum(w.byte_len), 0)::bigint
    INTO v_refs, v_live_bytes
    FROM sl_commit_work w
    WHERE w.accepted
      AND NOT w.is_dup;

    -- 7. Object row. Nothing accepted still records the key so replay stays stable, then GC can drop it.
    IF v_refs = 0 THEN
        INSERT INTO objects (
            id, key, size_bytes, broker_id, broker_epoch,
            live_refs, live_bytes, state, delete_after
        )
        VALUES (
            p_object_id, p_key, p_size, p_broker_id, p_broker_epoch,
            0, 0, 'DELETE_PENDING', now() + interval '5 minutes'
        );
    ELSE
        INSERT INTO objects (
            id, key, size_bytes, broker_id, broker_epoch,
            live_refs, live_bytes, state, delete_after
        )
        VALUES (
            p_object_id, p_key, p_size, p_broker_id, p_broker_epoch,
            v_refs, v_live_bytes, 'COMMITTED', NULL
        );

        INSERT INTO segments (
            topic_id, part, base_offset, last_offset, object_id,
            byte_start, byte_len, n_batches, max_ts
        )
        SELECT
            w.topic_id,
            w.part,
            w.base_off,
            w.base_off + w.n_records - 1,
            p_object_id,
            w.byte_start,
            w.byte_len,
            w.n_batches,
            w.max_ts
        FROM sl_commit_work w
        WHERE w.accepted
          AND NOT w.is_dup;
    END IF;

    -- 8. NOTIFY is delivered when the outer transaction commits, not before.
    PERFORM pg_notify(
        'sl_hw',
        n.topic_id::text || ':' || n.part::text || ':' || n.hw_off::text
    )
    FROM (
        SELECT DISTINCT w.topic_id, w.part, w.hw_off
        FROM sl_commit_work w
        WHERE w.accepted
          AND NOT w.is_dup
    ) n;

    RETURN QUERY
    SELECT w.entry_idx, COALESCE(w.base_off, -1), w.err_code, COALESCE(w.lso, -1)
    FROM sl_commit_work w
    ORDER BY w.entry_idx;
END;
$$;

CREATE OR REPLACE FUNCTION sl_commit_object(
    p_object_id uuid,
    p_key text,
    p_size bigint,
    p_broker_id text,
    p_broker_epoch bigint,
    p_topic_ids bigint[],
    p_parts int[],
    p_byte_starts bigint[],
    p_byte_lens int[],
    p_n_records int[],
    p_n_batches smallint[],
    p_max_ts bigint[],
    p_producer_ids bigint[],
    p_producer_epochs smallint[],
    p_first_seqs int[],
    p_last_seqs int[]
) RETURNS TABLE (idx int, base_offset bigint, error_code smallint, log_start_offset bigint)
LANGUAGE sql
AS $$
    SELECT * FROM sl_commit_object_impl(
        p_object_id, p_key, p_size, p_broker_id, p_broker_epoch,
        p_topic_ids, p_parts, p_byte_starts, p_byte_lens, p_n_records, p_n_batches, p_max_ts,
        p_producer_ids, p_producer_epochs, p_first_seqs, p_last_seqs,
        false
    );
$$;

-- Lab only. Same checks as sl_commit_object; offset assignment is one FOR UPDATE per entry.
CREATE OR REPLACE FUNCTION sl_commit_object_rowloop(
    p_object_id uuid,
    p_key text,
    p_size bigint,
    p_broker_id text,
    p_broker_epoch bigint,
    p_topic_ids bigint[],
    p_parts int[],
    p_byte_starts bigint[],
    p_byte_lens int[],
    p_n_records int[],
    p_n_batches smallint[],
    p_max_ts bigint[],
    p_producer_ids bigint[],
    p_producer_epochs smallint[],
    p_first_seqs int[],
    p_last_seqs int[]
) RETURNS TABLE (idx int, base_offset bigint, error_code smallint, log_start_offset bigint)
LANGUAGE sql
AS $$
    SELECT * FROM sl_commit_object_impl(
        p_object_id, p_key, p_size, p_broker_id, p_broker_epoch,
        p_topic_ids, p_parts, p_byte_starts, p_byte_lens, p_n_records, p_n_batches, p_max_ts,
        p_producer_ids, p_producer_epochs, p_first_seqs, p_last_seqs,
        true
    );
$$;

-- One transaction: topic row plus partition rows 0..n-1. A soft-deleted name can be reused (A2).
CREATE OR REPLACE FUNCTION sl_create_topic(
    p_name text,
    p_partitions int,
    p_retention_ms bigint,
    p_validate_only boolean
) RETURNS TABLE (error_code smallint, topic_id bigint)
LANGUAGE plpgsql
AS $$
DECLARE
    v_id bigint;
    v_exists smallint;
    v_partitions smallint;
BEGIN
    SELECT code INTO v_exists FROM sl_kafka_error_codes() WHERE name = 'TOPIC_ALREADY_EXISTS';
    SELECT code INTO v_partitions FROM sl_kafka_error_codes() WHERE name = 'INVALID_PARTITIONS';
    IF p_partitions IS NULL OR p_partitions < 1 THEN
        error_code := v_partitions;
        topic_id := NULL;
        RETURN NEXT;
        RETURN;
    END IF;
    IF EXISTS (SELECT 1 FROM topics WHERE name = p_name AND deleted_at IS NULL) THEN
        error_code := v_exists;
        topic_id := NULL;
        RETURN NEXT;
        RETURN;
    END IF;
    IF p_validate_only THEN
        error_code := 0;
        topic_id := NULL;
        RETURN NEXT;
        RETURN;
    END IF;
    BEGIN
        INSERT INTO topics (name, partitions, config)
        VALUES (
            p_name,
            p_partitions,
            jsonb_build_object('retention.ms', p_retention_ms, 'cleanup.policy', 'delete')
        )
        RETURNING id INTO v_id;
        INSERT INTO partitions (topic_id, part)
        SELECT v_id, gs FROM generate_series(0, p_partitions - 1) AS gs;
    EXCEPTION
        WHEN unique_violation THEN
            error_code := v_exists;
            topic_id := NULL;
            RETURN NEXT;
            RETURN;
    END;
    error_code := 0;
    topic_id := v_id;
    RETURN NEXT;
END;
$$;

-- Marks the topic deleted. Rows stay until retention cleanup (M8).
CREATE OR REPLACE FUNCTION sl_delete_topic(p_name text)
RETURNS smallint
LANGUAGE plpgsql
AS $$
DECLARE
    v_unknown smallint;
    v_updated int;
BEGIN
    SELECT code INTO v_unknown FROM sl_kafka_error_codes() WHERE name = 'UNKNOWN_TOPIC_OR_PARTITION';
    UPDATE topics
    SET deleted_at = now()
    WHERE name = p_name
      AND deleted_at IS NULL;
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated = 0 THEN
        RETURN v_unknown;
    END IF;
    RETURN 0;
END;
$$;
