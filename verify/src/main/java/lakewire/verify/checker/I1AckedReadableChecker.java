package lakewire.verify.checker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** I1: every acked record is readable at the offset the broker returned. */
public final class I1AckedReadableChecker {

    private I1AckedReadableChecker() {
    }

    public static void assertReadable(List<AckLog> acked, List<AckLog> read) {
        if (read.size() != acked.size()) {
            throw new AssertionError("read " + read.size() + " records, acked " + acked.size());
        }
        assertAllAckedPresent(acked, read);
    }

    /** I1 for the fault harness: extras in the final log are allowed; every ack must be present. */
    public static void assertAllAckedPresent(List<AckLog> acked, List<AckLog> read) {
        Map<String, AckLog> byKey = new HashMap<>();
        for (AckLog record : read) {
            AckLog previous = byKey.put(record.key(), record);
            if (previous != null
                && (previous.partition() != record.partition()
                    || previous.offset() != record.offset()
                    || previous.valueHash() != record.valueHash())) {
                throw new AssertionError("duplicate read key " + record.key());
            }
        }
        for (AckLog expected : acked) {
            AckLog actual = byKey.get(expected.key());
            if (actual == null
                || actual.partition() != expected.partition()
                || actual.offset() != expected.offset()
                || actual.valueHash() != expected.valueHash()) {
                throw new AssertionError("acked " + expected + " was read as " + actual);
            }
        }
    }
}
