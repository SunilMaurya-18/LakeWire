package lakewire.broker;

import java.nio.file.Files;
import lakewire.storage.LocalFsObjectStore;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ProduceIT {

    @Test
    void acksAllAndAcksOneCommitTenThousandRecords() throws Exception {
        try (ProduceCluster cluster = ProduceCluster.seaweed(ProduceCluster.fast())) {
            cluster.createTopic("acks-all", 3);
            cluster.createTopic("acks-one", 3);
            assertThat(cluster.produce("acks-all", "all", 10_000, 3)).isEqualTo(10_000);
            assertThat(cluster.produce("acks-one", "1", 10_000, 3)).isEqualTo(10_000);
            cluster.assertCommitted("acks-all", 10_000, 3);
            cluster.assertCommitted("acks-one", 10_000, 3);
            try (var files = Files.list(cluster.dataDir())) {
                assertThat(files.toList()).isEmpty();
            }
        }
    }
}
