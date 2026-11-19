package lakewire.verify;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lakewire.testkit.LakeWireCluster;
import lakewire.verify.checker.AckLog;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;

@Order(3)
class BrokerKillIT {

    private static final Logger log = LoggerFactory.getLogger(BrokerKillIT.class);

    @Test
    void clientsResumeWithinFifteenSecondsAfterABrokerIsKilled() throws Exception {
        try (LakeWireCluster cluster = LakeWireCluster.start(3)) {
            ClusterIO.createTopic(cluster.bootstrap(1), "orders");
            CopyOnWriteArrayList<AckLog> acked = new CopyOnWriteArrayList<>();
            AtomicInteger next = new AtomicInteger();
            AtomicBoolean running = new AtomicBoolean(true);
            Thread writer = Thread.ofPlatform().name("kill-writer").start(() -> {
                try (KafkaProducer<String, String> producer = new KafkaProducer<>(
                    ClusterIO.producerConfig(cluster.bootstrapAll())
                )) {
                    while (running.get()) {
                        int id = next.getAndIncrement();
                        String key = "k-" + id;
                        String value = "v-" + id;
                        producer.send(new ProducerRecord<>("orders", 0, key, value), (metadata, error) -> {
                            if (error == null) {
                                acked.add(new AckLog(key, value.hashCode(), metadata.partition(), metadata.offset()));
                            }
                        });
                        Thread.sleep(50);
                    }
                    producer.flush();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            });
            try {
                Awaitility.await().atMost(ofSeconds(30)).until(() -> acked.size() >= 5);
                int before = acked.size();
                long started = System.nanoTime();
                cluster.kill(0);
                Awaitility.await().atMost(ofSeconds(15)).until(() -> acked.size() > before);
                long recoveryMs = (System.nanoTime() - started) / 1_000_000L;
                log.info("broker kill recovery {} ms", recoveryMs);
                assertThat(recoveryMs).isLessThanOrEqualTo(15_000L);
            } finally {
                running.set(false);
                writer.join(30_000);
            }
            ClusterIO.assertReadable(List.copyOf(acked), cluster.bootstrap(1), "orders");
        }
    }
}
