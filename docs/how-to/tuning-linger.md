# Tuning linger

Produce waits up to `LAKEWIRE_LINGER_MS` (default **200**) or until `LAKEWIRE_OBJECT_MAX_BYTES` (default **8 MiB**) before one PUT + `sl_commit_object`.

- Lower linger (20–50 ms) cuts produce P50 and raises PUT count (cost).
- Higher linger batches more partitions into one object (A20: one Produce payload = one RecordBatch = one commit entry).
- `LAKEWIRE_BUFFER_MAX_BYTES` (64 MiB) and `LAKEWIRE_UPLOAD_CONCURRENCY` (4) cap in-flight objects. A full buffer fails the partition with `REQUEST_TIMED_OUT` (A35).

Ack is sent only after the object is in storage **and** the commit transaction returned.
