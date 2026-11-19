package lakewire.verify.nemesis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.model.Toxic;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import lakewire.testkit.LakeWireCluster;
import lakewire.verify.Scheduler;
import lakewire.verify.Workload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ClusterFaults {

    private static final Logger log = LoggerFactory.getLogger(ClusterFaults.class);

    private ClusterFaults() {
    }

    public static void apply(LakeWireCluster cluster, Scheduler.Event event) {
        log.info("nemesis {} broker={} holdMs={}", event.kind().flag(), event.broker(), event.holdMs());
        try {
            switch (event.kind()) {
                case KILL -> kill(cluster, event);
                case PAUSE -> pause(cluster, event);
                case PARTITION_PG -> partition(cluster.pgProxy(), event, true);
                case SLOW_S3 -> slowS3(cluster.s3Proxy(), event);
                case PG_RESTART -> cluster.restartPostgres();
                case RETENTION_STORM -> retention(cluster);
            }
        } catch (Exception failure) {
            log.warn("nemesis {} failed: {}", event.kind().flag(), failure.toString());
        }
    }

    private static void kill(LakeWireCluster cluster, Scheduler.Event event) throws InterruptedException {
        cluster.kill(event.broker());
        Thread.sleep(Math.min(event.holdMs(), 5_000));
        cluster.startBroker(event.broker());
    }

    private static void pause(LakeWireCluster cluster, Scheduler.Event event) throws InterruptedException {
        cluster.pause(event.broker());
        Thread.sleep(event.holdMs());
        cluster.unpause(event.broker());
    }

    private static void partition(Proxy proxy, Scheduler.Event event, boolean reset) throws IOException, InterruptedException {
        Toxic toxic = reset
            ? proxy.toxics().resetPeer("verify-reset", ToxicDirection.DOWNSTREAM, 0)
            : proxy.toxics().timeout("verify-timeout", ToxicDirection.DOWNSTREAM, 1_000);
        try {
            Thread.sleep(event.holdMs());
        } finally {
            toxic.remove();
        }
    }

    private static void slowS3(Proxy proxy, Scheduler.Event event) throws IOException, InterruptedException {
        Toxic toxic = proxy.toxics().latency("verify-slow-s3", ToxicDirection.DOWNSTREAM, 1_500);
        try {
            Thread.sleep(event.holdMs());
        } finally {
            toxic.remove();
        }
    }

    private static void retention(LakeWireCluster cluster) throws Exception {
        try (Connection connection = DriverManager.getConnection(
            cluster.jdbcUrl(), cluster.user(), cluster.password());
             PreparedStatement statement = connection.prepareStatement(
                 "SELECT sl_set_topic_retention(?, ?)")) {
            statement.setString(1, Workload.TOPIC);
            statement.setLong(2, 2_000);
            statement.executeQuery().close();
        }
    }

    public static Duration pauseFloor() {
        return Duration.ofSeconds(4);
    }
}
