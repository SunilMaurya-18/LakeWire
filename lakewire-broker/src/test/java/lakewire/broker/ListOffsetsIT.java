package lakewire.broker;

import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import lakewire.storage.LocalFsObjectStore;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ListOffsetsIT {

    @Test
    void earliestLatestAndTimestampLookupsFollowTheHighWatermark() throws Exception {
        java.nio.file.Path data = Files.createTempDirectory("lakewire-offsets");
        try (ProduceCluster cluster = ProduceCluster.open(new LocalFsObjectStore(data), ProduceCluster.fast())) {
            cluster.createTopic("offsets", 1);
            assertThat(cluster.produce("offsets", "all", 20, 1)).isEqualTo(20);
            TopicPartition partition = new TopicPartition("offsets", 0);
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:" + cluster.port(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false",
                ConsumerConfig.CLIENT_ID_CONFIG, "offsets-it"
            ))) {
                assertThat(consumer.beginningOffsets(List.of(partition)).get(partition)).isZero();
                assertThat(consumer.endOffsets(List.of(partition)).get(partition)).isEqualTo(20L);
                OffsetAndTimestamp found = consumer.offsetsForTimes(Map.of(partition, 0L)).get(partition);
                assertThat(found).isNotNull();
                assertThat(found.offset()).isZero();
                assertThat(consumer.offsetsForTimes(Map.of(partition, Long.MAX_VALUE)).get(partition)).isNull();
            }
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
}
