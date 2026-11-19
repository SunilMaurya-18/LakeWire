package lakewire.coordinator;

/** Result of {@code sl_init_producer_id}. {@code errorCode} 0 means {@code producerId} and {@code epoch} are live. */
public record ProducerIdAllocation(short errorCode, long producerId, short epoch) {
}
