package lakewire.cli;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
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
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

class DevModeIT {

    private static final Logger log = LoggerFactory.getLogger(DevModeIT.class);

    @Test
    @Timeout(180)
    void produceAndConsumeThroughEmbeddedDev(@TempDir Path home) throws Exception {
        try (DevServer server = DevServer.start(home, 0, 0)) {
            log.info("lakewire dev ready in {} ms", server.readyMs());
            assertThat(server.readyMs()).isLessThan(30_000);
            String bootstrap = "127.0.0.1:" + server.port();
            try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000",
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "20000",
                AdminClientConfig.CLIENT_ID_CONFIG, "dev-mode-it"
            ))) {
                admin.createTopics(List.of(new NewTopic("dev-it", 1, (short) 1)))
                    .all()
                    .get(20, TimeUnit.SECONDS);
            }
            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
            producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
            producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000");
            producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "20000");
            producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "dev-mode-it");
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
                producer.send(new ProducerRecord<>("dev-it", 0, "k", "hello-dev")).get(20, TimeUnit.SECONDS);
            }
            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            consumerProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000");
            consumerProps.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "20000");
            consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "dev-mode-it-c");
            TopicPartition partition = new TopicPartition("dev-it", 0);
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                consumer.assign(List.of(partition));
                consumer.seekToBeginning(List.of(partition));
                Awaitility.await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                    var records = consumer.poll(Duration.ofMillis(200));
                    assertThat(records).extracting(ConsumerRecord::value).contains("hello-dev");
                });
            }
        }
    }
}
