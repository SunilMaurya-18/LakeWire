package lakewire.coordinator;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class CoordinatorModuleTest {

    @Test
    void moduleIsOnTheClasspath() {
        assertThat(CoordinatorModuleTest.class.getPackageName()).isEqualTo("lakewire.coordinator");
    }
}
