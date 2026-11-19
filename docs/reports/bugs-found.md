# Bugs found by the verification harness

## I2 treated log start as offset 0

Retention can raise `log_start_offset` while leaving a contiguous suffix of segments. The I2 checker walked spans from offset 0, so a legal prefix deletion looked like a hole. It now starts at `partitions.log_start_offset`.
