package lakewire.broker.read;

import java.util.List;

@FunctionalInterface
public interface SegmentSource {
    List<StoredSegment> segments(long topicId, int part);
}
