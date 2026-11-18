package lakewire.broker;

import java.nio.file.Files;
import java.time.Duration;
import lakewire.storage.FaultInjectingObjectStore;
import lakewire.storage.LocalFsObjectStore;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ProduceFaultIT {

    private static final long SEED = 42L;

    @Test
    void injectedPutFailuresStillCommitEachAckOnce() throws Exception {
        PathStore store = PathStore.open();
        FaultInjectingObjectStore faults = new FaultInjectingObjectStore(
            store.store(), SEED, 0.30, Duration.ZERO, 0);
        try (ProduceCluster cluster = ProduceCluster.open(faults, ProduceCluster.fast())) {
            cluster.createTopic("faults", 3);
            int acked = cluster.produce("faults", "all", 1_000, 3);
            assertThat(acked).as("seed %s", SEED).isEqualTo(1_000);
            cluster.assertCommitted("faults", 1_000, 3);
        } finally {
            store.delete();
        }
    }

    private record PathStore(java.nio.file.Path path, LocalFsObjectStore store) {
        static PathStore open() throws Exception {
            java.nio.file.Path path = Files.createTempDirectory("lakewire-fault");
            return new PathStore(path, new LocalFsObjectStore(path));
        }

        void delete() throws Exception {
            try (var paths = Files.walk(path)) {
                paths.sorted(java.util.Comparator.reverseOrder()).forEach(file -> {
                    try {
                        Files.deleteIfExists(file);
                    } catch (Exception ignored) {
                        // the broker already closed the store
                    }
                });
            }
        }
    }
}
