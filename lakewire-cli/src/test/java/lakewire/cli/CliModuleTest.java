package lakewire.cli;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class CliModuleTest {

    @Test
    void moduleIsOnTheClasspath() {
        assertThat(CliModuleTest.class.getPackageName()).isEqualTo("lakewire.cli");
    }
}
