package lakewire.broker.read;

/** High watermark and log start for one partition. Both come from Postgres. */
public record Watermark(long highWatermark, long logStartOffset) {
}
