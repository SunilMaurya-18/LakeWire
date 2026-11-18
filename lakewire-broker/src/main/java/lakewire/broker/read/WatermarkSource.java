package lakewire.broker.read;

@FunctionalInterface
public interface WatermarkSource {
    Watermark watermark(long topicId, int part);
}
