package lakewire.storage;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class FaultInjectingObjectStoreTest {

    @TempDir
    java.nio.file.Path leftRoot;

    @TempDir
    java.nio.file.Path rightRoot;

    @Test
    void sameSeedProducesTheSameFaultSequence() throws Exception {
        long seed = 7L;
        try (ObjectStore left = faults(new LocalFsObjectStore(leftRoot), seed, Duration.ZERO);
             ObjectStore right = faults(new LocalFsObjectStore(rightRoot), seed, Duration.ZERO)) {
            int putFailures = 0;
            int getFailures = 0;
            for (int i = 0; i < 40; i++) {
                String key = "k-" + i;
                byte[] body = new byte[] {(byte) i, 9};
                String put = outcome(() -> {
                    StorageAwait.await(left.put(key, body));
                    return "put";
                });
                String putAgain = outcome(() -> {
                    StorageAwait.await(right.put(key, body));
                    return "put";
                });
                assertThat(putAgain).isEqualTo(put);
                if (put.startsWith("error:")) {
                    putFailures++;
                }
                String read = outcome(() -> bytes(StorageAwait.await(left.getRange(key, 0, body.length))));
                String readAgain = outcome(() -> bytes(StorageAwait.await(right.getRange(key, 0, body.length))));
                assertThat(readAgain).isEqualTo(read);
                if (read.startsWith("error:")) {
                    getFailures++;
                }
            }
            assertThat(putFailures).isPositive();
            assertThat(getFailures).isPositive();
        }
    }

    @Test
    void extraPutLatencyIsApplied() {
        try (ObjectStore store = new LocalFsObjectStore(leftRoot);
             ObjectStore faults = new FaultInjectingObjectStore(store, 1L, 0.0, Duration.ofMillis(200), 0.0)) {
            long started = System.nanoTime();
            StorageAwait.await(faults.put("slow", new byte[] {1}));
            long elapsedMs = (System.nanoTime() - started) / 1_000_000;
            assertThat(elapsedMs).isGreaterThanOrEqualTo(150);
        }
    }

    private static FaultInjectingObjectStore faults(ObjectStore delegate, long seed, Duration latency) {
        return new FaultInjectingObjectStore(delegate, seed, 0.5, latency, 0.5);
    }

    private static String outcome(RunnableWithOutcome call) {
        try {
            return "ok:" + call.run();
        } catch (RuntimeException failure) {
            return "error:" + failure.getClass().getSimpleName();
        }
    }

    private static String bytes(byte[] body) {
        return java.util.Arrays.toString(body);
    }

    @FunctionalInterface
    private interface RunnableWithOutcome {
        String run();
    }
}
