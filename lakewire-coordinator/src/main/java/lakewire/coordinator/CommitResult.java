package lakewire.coordinator;

public record CommitResult(int idx, long baseOffset, short errorCode, long logStartOffset) {
}
