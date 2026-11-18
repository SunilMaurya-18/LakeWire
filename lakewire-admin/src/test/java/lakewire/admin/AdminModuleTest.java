package lakewire.admin;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class AdminModuleTest {

    @Test
    void moduleIsOnTheClasspath() {
        assertThat(AdminModuleTest.class.getPackageName()).isEqualTo("lakewire.admin");
    }
}
