package lakewire.coordinator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;

class CoordinatorModuleTest {

    @Test
    void moduleIsOnTheClasspath() {
        assertThat(CoordinatorModuleTest.class.getPackageName()).isEqualTo("lakewire.coordinator");
    }

    @Test
    void configReadsEnvironmentAndKeepsGroupCommitOffByDefault() {
        CoordinatorConfig config = CoordinatorConfig.fromEnvironment(Map.of(
            "LAKEWIRE_PG_URL", "jdbc:postgresql://localhost/lakewire",
            "LAKEWIRE_PG_USER", "lakewire",
            "LAKEWIRE_PG_PASSWORD", "lakewire"
        ));

        assertThat(config.migrate()).isTrue();
        assertThat(config.leaseMs()).isEqualTo(10_000);
        assertThat(config.heartbeatMs()).isEqualTo(2_000);
        assertThat(config.groupCommit()).isFalse();
    }

    @Test
    void configRejectsAMissingUrl() {
        assertThatThrownBy(() -> CoordinatorConfig.fromEnvironment(Map.of(
            "LAKEWIRE_PG_USER", "lakewire",
            "LAKEWIRE_PG_PASSWORD", "lakewire"
        ))).isInstanceOf(IllegalArgumentException.class);
    }
}
