package lakewire.verify.checker;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lakewire.storage.ObjectStore;

/** I5: an idempotent producer's retries do not leave two copies of the same sequence. */
public final class I5NoDuplicateChecker {

    private I5NoDuplicateChecker() {
    }

    public static void assertUnique(ObjectStore store, String jdbcUrl, String user, String password, String topic)
        throws Exception {
        List<StoredSequences.Sequence> sequences = StoredSequences.load(store, jdbcUrl, user, password, topic);
        if (sequences.isEmpty()) {
            throw new AssertionError("topic " + topic + " has no idempotent sequences");
        }
        Set<StoredSequences.Sequence> seen = new HashSet<>();
        for (StoredSequences.Sequence sequence : sequences) {
            if (!seen.add(sequence)) {
                throw new AssertionError("duplicate sequence " + sequence.sequence()
                    + " on partition " + sequence.partition()
                    + " producer " + sequence.producerId());
            }
        }
    }
}
