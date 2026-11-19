package lakewire.verify.checker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lakewire.storage.ObjectStore;

/** I3: per producer and partition, sequence numbers increase by one in offset order. */
public final class I3SequenceOrderChecker {

    private I3SequenceOrderChecker() {
    }

    public static void assertMonotonic(ObjectStore store, String jdbcUrl, String user, String password, String topic)
        throws Exception {
        List<StoredSequences.Sequence> sequences = StoredSequences.load(store, jdbcUrl, user, password, topic);
        if (sequences.isEmpty()) {
            throw new AssertionError("topic " + topic + " has no idempotent sequences");
        }
        Map<Key, Integer> previous = new HashMap<>();
        for (StoredSequences.Sequence sequence : sequences) {
            Key key = new Key(sequence.partition(), sequence.producerId());
            Integer last = previous.get(key);
            if (last != null) {
                int expected = last == Integer.MAX_VALUE ? 0 : last + 1;
                if (sequence.sequence() != expected) {
                    throw new AssertionError("partition " + sequence.partition()
                        + " producer " + sequence.producerId()
                        + " sequence " + sequence.sequence()
                        + " follows " + last);
                }
            }
            previous.put(key, sequence.sequence());
        }
    }

    private record Key(int partition, long producerId) {
    }
}
