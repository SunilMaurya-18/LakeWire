package lakewire.verify;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lakewire.testkit.LakeWireCluster;
import lakewire.verify.checker.AckLog;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@Order(2)
class ZombieBrokerIT {

    @Test
    void pausedBrokerLateCommitIsRejectedAndAckedRecordsRemain() throws Exception {
        Map<String, String> env = Map.of(
            "LAKEWIRE_LEASE_MS", "3000",
            "LAKEWIRE_HEARTBEAT_MS", "500",
            "LAKEWIRE_LINGER_MS", "4000"
        );
        try (LakeWireCluster cluster = LakeWireCluster.start(3, env)) {
            ClusterIO.createTopic(cluster.bootstrap(0), "orders");
            Map<String, Object> config = ClusterIO.producerConfig(cluster.bootstrap(0));
            config.put(ProducerConfig.CLIENT_ID_CONFIG, "zombie,az=a");
            config.put(ProducerConfig.RETRIES_CONFIG, 0);
            config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
            config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 45_000);
            config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 20_000);
            CompletableFuture<Exception> failed = new CompletableFuture<>();
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
                producer.send(new ProducerRecord<>("orders", 0, "zombie", "late"), (metadata, error) -> {
                    if (error == null) {
                        failed.complete(null);
                    } else {
                        failed.complete(error);
                    }
                });
                Awaitility.await().pollDelay(Duration.ofMillis(200)).atMost(Duration.ofSeconds(2)).until(() -> true);
                cluster.pause(0);
                Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> !leaseLive(cluster, "1"));
                Awaitility.await().pollDelay(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(2)).until(() -> true);
                cluster.unpause(0);
                Exception error = failed.get(40, TimeUnit.SECONDS);
                assertThat(error).isInstanceOf(RetriableException.class);
            }
            Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> epoch(cluster, "1") > 1);
            var acked = ClusterIO.produce(cluster.bootstrap(1), "orders", 5, "check,az=b");
            List<AckLog> read = ClusterIO.consume(cluster.bootstrap(2), "orders", acked.size(), "read,az=c");
            assertThat(read).extracting(AckLog::key).doesNotContain("zombie");
            lakewire.verify.checker.I1AckedReadableChecker.assertReadable(acked, read);
        }
    }

    private static long epoch(LakeWireCluster cluster, String brokerId) throws Exception {
        try (Connection connection = DriverManager.getConnection(cluster.jdbcUrl(), cluster.user(), cluster.password());
             Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT epoch FROM brokers WHERE id = '" + brokerId + "'")) {
            assertThat(rows.next()).isTrue();
            return rows.getLong(1);
        }
    }

    private static boolean leaseLive(LakeWireCluster cluster, String brokerId) throws Exception {
        try (Connection connection = DriverManager.getConnection(cluster.jdbcUrl(), cluster.user(), cluster.password());
             Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery(
                 "SELECT lease_expires_at > now() FROM brokers WHERE id = '" + brokerId + "'")) {
            return rows.next() && rows.getBoolean(1);
        }
    }
}
