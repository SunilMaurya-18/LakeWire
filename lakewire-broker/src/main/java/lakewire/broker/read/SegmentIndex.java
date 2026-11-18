package lakewire.broker.read;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.util.ArrayList;
import java.util.List;

/** Immutable segment rows per partition, keyed so a 404 can drop one partition and reload it. */
public final class SegmentIndex {

    private final LoadingCache<Key, List<StoredSegment>> cache;

    public SegmentIndex(SegmentSource source) {
        this.cache = Caffeine.newBuilder()
            .maximumSize(100_000)
            .build(key -> source.segments(key.topicId, key.part));
    }

    public List<StoredSegment> all(long topicId, int part) {
        return cache.get(new Key(topicId, part));
    }

    public List<StoredSegment> covering(long topicId, int part, long fetchOffset, int maxBytes) {
        List<StoredSegment> chosen = new ArrayList<>();
        int bytes = 0;
        for (StoredSegment segment : all(topicId, part)) {
            if (segment.lastOffset() < fetchOffset) {
                continue;
            }
            chosen.add(segment);
            bytes += segment.byteLen();
            if (bytes >= Math.max(maxBytes, 1)) {
                break;
            }
        }
        return chosen;
    }

    public void remember(long topicId, int part, List<StoredSegment> segments) {
        cache.put(new Key(topicId, part), List.copyOf(segments));
    }

    public void invalidate(long topicId, int part) {
        cache.invalidate(new Key(topicId, part));
    }

    private record Key(long topicId, int part) {
    }
}
