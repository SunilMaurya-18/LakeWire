package lakewire.bench;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class BenchModuleTest {

    @Test
    void moduleIsOnTheClasspath() {
        assertThat(BenchModuleTest.class.getPackageName()).isEqualTo("lakewire.bench");
    }
}
