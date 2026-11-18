package lakewire.coordinator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BrokerRegistryIT {

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

    @BeforeEach
    void reset() {
        fixture.truncate();
    }

    @Test
    void reRegisterIncrementsEpochAndOldHeartbeatIsFenced() {
        long first = fixture.registry.register("1", "az1", "127.0.0.1:9092");
        assertThat(fixture.registry.heartbeat("1", first)).isTrue();

        long second = fixture.registry.register("1", "az2", "127.0.0.1:9093");

        assertThat(second).isEqualTo(first + 1);
        assertThat(fixture.registry.heartbeat("1", first)).isFalse();
        assertThat(fixture.registry.heartbeat("1", second)).isTrue();
        assertThat(fixture.queryString("SELECT zone FROM brokers WHERE id = '1'")).isEqualTo("az2");
        assertThat(fixture.registry.leaseMs()).isEqualTo(CoordinatorConfig.DEFAULT_LEASE_MS);
        assertThat(fixture.registry.heartbeatIntervalMs()).isEqualTo(CoordinatorConfig.DEFAULT_HEARTBEAT_MS);
    }
}
