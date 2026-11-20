package lakewire.coordinator;

public record BrokerSnapshot(String id, String zone, String endpoint, long epoch, boolean live) {
}
