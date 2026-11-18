package lakewire.coordinator;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaErrorCodesIT {

    private static CoordinatorFixture fixture;

    @BeforeAll
    static void start() {
        fixture = CoordinatorFixture.start();
    }

    @AfterAll
    static void stop() {
        if (fixture != null) {
            fixture.close();
        }
    }

    @Test
    void codesMatchKafkaClients() throws Exception {
        Set<String> names = new HashSet<>();
        try (Connection connection = fixture.coordinator.dataSource().getConnection();
             Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT name, code FROM sl_kafka_error_codes() ORDER BY name")) {
            while (rows.next()) {
                String name = rows.getString(1);
                short code = rows.getShort(2);
                names.add(name);
                assertThat(code)
                    .as(name)
                    .isEqualTo(Errors.valueOf(name).code());
            }
        }
        assertThat(names).contains(
            "UNKNOWN_TOPIC_OR_PARTITION",
            "NOT_LEADER_OR_FOLLOWER",
            "REQUEST_TIMED_OUT",
            "OUT_OF_ORDER_SEQUENCE_NUMBER",
            "INVALID_PRODUCER_EPOCH",
            "UNKNOWN_PRODUCER_ID"
        );
    }

    @Test
    void clusterIdIsStableBase64Url() {
        String clusterId = fixture.queryString("SELECT cluster_id FROM cluster WHERE id = 1");
        assertThat(clusterId).matches("[A-Za-z0-9_-]{22}");
    }
}
