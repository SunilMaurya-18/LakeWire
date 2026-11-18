package lakewire.verify.checker;

/** One acked or read record. The value is stored as a hash so the checker does not keep the payload. */
public record AckLog(String key, int valueHash, int partition, long offset) {
}
