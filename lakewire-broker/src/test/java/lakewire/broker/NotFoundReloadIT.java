package lakewire.broker;

import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lakewire.storage.LocalFsObjectStore;
import lakewire.storage.ObjectHead;
import lakewire.storage.ObjectNotFoundException;
import lakewire.storage.ObjectStore;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NotFoundReloadIT {

    @Test
    void aMissingObjectIsReloadedOnceAndThenRead() throws Exception {
        java.nio.file.Path data = Files.createTempDirectory("lakewire-404");
        OnceMissing store = new OnceMissing(new LocalFsObjectStore(data));
        try (ProduceCluster cluster = ProduceCluster.open(store, ProduceCluster.fast())) {
            cluster.createTopic("reload", 1);
            assertThat(cluster.produce("reload", "all", 5, 1)).isEqualTo(5);
            try (KafkaConsumer<String, String> consumer = consumer(cluster.port())) {
                TopicPartition partition = new TopicPartition("reload", 0);
                consumer.assign(List.of(partition));
                consumer.seekToBeginning(List.of(partition));
                int seen = 0;
                long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
                while (seen < 5 && System.nanoTime() < deadline) {
                    seen += consumer.poll(Duration.ofMillis(200)).count();
                }
                assertThat(seen).isEqualTo(5);
            }
            assertThat(store.misses).isGreaterThan(0);
        } finally {
            try (var paths = Files.walk(data)) {
                paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (Exception ignored) {
                        // closed with the broker
                    }
                });
            }
        }
    }

    private static KafkaConsumer<String, String> consumer(int port) {
        return new KafkaConsumer<>(Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:" + port,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
            ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false",
            ConsumerConfig.CLIENT_ID_CONFIG, "reload-it"
        ));
    }

    private static final class OnceMissing implements ObjectStore {
        private final ObjectStore delegate;
        private final Set<String> missed = ConcurrentHashMap.newKeySet();
        private int misses;

        private OnceMissing(ObjectStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableFuture<Void> put(String key, byte[] body) {
            return delegate.put(key, body);
        }

        @Override
        public synchronized CompletableFuture<byte[]> getRange(String key, long start, int length) {
            if (missed.add(key)) {
                misses++;
                return CompletableFuture.failedFuture(new ObjectNotFoundException(key));
            }
            return delegate.getRange(key, start, length);
        }

        @Override
        public CompletableFuture<ObjectHead> head(String key) {
            return delegate.head(key);
        }

        @Override
        public CompletableFuture<Void> delete(String key) {
            return delegate.delete(key);
        }

        @Override
        public CompletableFuture<List<String>> list(String prefix) {
            return delegate.list(prefix);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
