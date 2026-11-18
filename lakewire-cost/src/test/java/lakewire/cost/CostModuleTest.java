package lakewire.cost;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class CostModuleTest {

    @Test
    void moduleIsOnTheClasspath() {
        assertThat(CostModuleTest.class.getPackageName()).isEqualTo("lakewire.cost");
    }
}
