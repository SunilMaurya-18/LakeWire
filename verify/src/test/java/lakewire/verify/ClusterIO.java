package lakewire.verify;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import lakewire.verify.checker.AckLog;
import lakewire.verify.checker.I1AckedReadableChecker;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import static org.assertj.core.api.Assertions.assertThat;

/** Produce and fetch helpers for the multi-broker tests. */
final class ClusterIO {

    private ClusterIO() {
    }

    static void createTopic(String bootstrap, String topic) throws Exception {
        try (Admin admin = Admin.create(adminConfig(bootstrap, "create"))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
        }
    }

    static Map<String, Object> adminConfig(String bootstrap, String clientId) {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(AdminClientConfig.CLIENT_ID_CONFIG, clientId);
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000");
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000");
        return config;
    }

    static Map<String, Object> producerConfig(String bootstrap) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20_000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60_000);
        config.put(ProducerConfig.RETRIES_CONFIG, 5);
        config.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 50);
        config.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 200);
        return config;
    }

    static List<AckLog> produce(String bootstrap, String topic, int records) {
        return produce(bootstrap, topic, records, "write");
    }

    static List<AckLog> produce(String bootstrap, String topic, int records, String clientId) {
        List<AckLog> acked = new CopyOnWriteArrayList<>();
        List<Exception> errors = new CopyOnWriteArrayList<>();
        Map<String, Object> config = producerConfig(bootstrap);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            for (int i = 0; i < records; i++) {
                String key = "k-" + i;
                String value = "v-" + i;
                producer.send(new ProducerRecord<>(topic, 0, key, value), (metadata, failure) -> {
                    if (failure != null) {
                        errors.add(failure);
                    } else {
                        acked.add(new AckLog(key, value.hashCode(), metadata.partition(), metadata.offset()));
                    }
                });
            }
            producer.flush();
        }
        assertThat(errors).isEmpty();
        assertThat(acked).hasSize(records);
        return acked;
    }

    static List<AckLog> consume(String bootstrap, String topic, int expected) {
        return consume(bootstrap, topic, expected, "read");
    }

    static List<AckLog> consume(String bootstrap, String topic, int expected, String clientId) {
        List<AckLog> read = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
            ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false",
            ConsumerConfig.CLIENT_ID_CONFIG, clientId
        ))) {
            List<TopicPartition> assigned = List.of(new TopicPartition(topic, 0));
            consumer.assign(assigned);
            consumer.seekToBeginning(assigned);
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (read.size() < expected && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(400))) {
                    read.add(new AckLog(record.key(), record.value().hashCode(), record.partition(), record.offset()));
                }
            }
        }
        return read;
    }

    static void assertReadable(List<AckLog> acked, String bootstrap, String topic) {
        I1AckedReadableChecker.assertReadable(acked, consume(bootstrap, topic, acked.size()));
    }
}
