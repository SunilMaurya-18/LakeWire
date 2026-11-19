# Bugs found by the verification harness

## Fetch hid committed records behind a stale HW cache

After a broker↔Postgres partition, `LISTEN sl_hw` drops notifications. Fetch only re-read Postgres when the requested offset was at or above the cached HW, so a fetch from 0 used a stale watermark and stopped early. The verify harness then reported I1 lost writes while `partitions.next_offset` still matched the acks. Fetch now reads the watermark from Postgres on every request.

## I2 treated log start as offset 0

Retention can raise `log_start_offset` while leaving a contiguous suffix of segments. The I2 checker walked spans from offset 0, so a legal prefix deletion looked like a hole. It now starts at `partitions.log_start_offset`.
