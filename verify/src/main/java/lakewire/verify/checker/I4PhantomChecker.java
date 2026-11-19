package lakewire.verify.checker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * I4: no read of an offset at or above the committed HW, and no phantom
 * (read then missing from the final log).
 */
public final class I4PhantomChecker {

    private I4PhantomChecker() {
    }

    public static void assertStable(List<AckLog> reads, List<AckLog> finalLog, Map<Integer, Long> highWatermark) {
        Map<String, AckLog> byKey = new HashMap<>();
        Map<String, AckLog> byOffset = new HashMap<>();
        for (AckLog record : finalLog) {
            byKey.put(record.key(), record);
            byOffset.put(record.partition() + ":" + record.offset(), record);
        }
        for (AckLog read : reads) {
            Long hw = highWatermark.get(read.partition());
            if (hw != null && read.offset() >= hw) {
                throw new AssertionError("I4 read offset " + read.offset() + " >= HW " + hw
                    + " on partition " + read.partition());
            }
            AckLog stored = byOffset.get(read.partition() + ":" + read.offset());
            if (stored == null) {
                throw new AssertionError("I4 phantom read " + read);
            }
            if (stored.valueHash() != read.valueHash() || !stored.key().equals(read.key())) {
                throw new AssertionError("I4 read " + read + " does not match final " + stored);
            }
            AckLog keyed = byKey.get(read.key());
            if (keyed == null) {
                throw new AssertionError("I4 phantom key " + read.key());
            }
        }
    }
}
