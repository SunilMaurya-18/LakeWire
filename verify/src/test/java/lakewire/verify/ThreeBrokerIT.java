package lakewire.verify;

import java.util.List;
import java.util.concurrent.TimeUnit;
import lakewire.testkit.LakeWireCluster;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@Order(1)
class ThreeBrokerIT {

    @Test
    void zoneBClientGetsZoneBLeaderAndAnotherBrokerCanFetch() throws Exception {
        try (LakeWireCluster cluster = LakeWireCluster.start(3)) {
            ClusterIO.createTopic(cluster.bootstrap(0), "orders");
            try (Admin admin = Admin.create(ClusterIO.adminConfig(cluster.bootstrap(0), "x,az=b"))) {
                TopicDescription topic = admin.describeTopics(List.of("orders"))
                    .allTopicNames()
                    .get(20, TimeUnit.SECONDS)
                    .get("orders");
                assertThat(topic.partitions()).hasSize(1);
                assertThat(topic.partitions().get(0).leader().id()).isEqualTo(cluster.brokerId(1));
            }
            var acked = ClusterIO.produce(cluster.bootstrap(0), "orders", 20);
            ClusterIO.assertReadable(acked, cluster.bootstrap(2), "orders");
        }
    }
}
