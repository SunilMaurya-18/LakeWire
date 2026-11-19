package lakewire.broker;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MetricsEndpointIT {

    private static final Set<String> REQUIRED = Set.of(
        "lakewire_commit_latency_seconds",
        "lakewire_produce_latency_seconds",
        "lakewire_fetch_latency_seconds",
        "lakewire_s3_put_total",
        "lakewire_s3_get_total",
        "lakewire_cache_hits_total",
        "lakewire_cache_misses_total",
        "lakewire_object_bytes",
        "lakewire_buffer_bytes",
        "lakewire_audit_violations_total"
    );

    @Test
    void metricsEndpointExposesEveryM10Name() throws Exception {
        try (ProduceCluster cluster = ProduceCluster.seaweed(ProduceCluster.fast())) {
            cluster.createTopic("metrics", 1);
            assertThat(cluster.produce("metrics", "all", 32, 1)).isEqualTo(32);
            consumeTwice(cluster.port());
            String body = scrape(cluster.metricsPort());
            for (String name : REQUIRED) {
                assertThat(body).contains(name);
            }
        }
    }

    private static void consumeTwice(int port) {
        Map<String, Object> config = Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:" + port,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.CLIENT_ID_CONFIG, "metrics-it",
            ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15_000,
            ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 15_000
        );
        TopicPartition partition = new TopicPartition("metrics", 0);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            consumer.poll(Duration.ofSeconds(10));
            consumer.seekToBeginning(List.of(partition));
            consumer.poll(Duration.ofSeconds(10));
        }
    }

    private static String scrape(int port) throws Exception {
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/metrics"))
            .timeout(Duration.ofSeconds(5))
            .GET()
            .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return response.body();
    }
}
