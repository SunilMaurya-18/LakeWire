package lakewire.workers;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class WorkersModuleTest {

    @Test
    void moduleIsOnTheClasspath() {
        assertThat(WorkersModuleTest.class.getPackageName()).isEqualTo("lakewire.workers");
    }
}
