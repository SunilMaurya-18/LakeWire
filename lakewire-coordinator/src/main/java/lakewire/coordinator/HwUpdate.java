package lakewire.coordinator;

public record HwUpdate(long topicId, int part, long nextOffset) {
}
