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
        Map<String, AckLog> byKey = new HashMap<>();
        for (AckLog record : read) {
            if (byKey.put(record.key(), record) != null) {
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
