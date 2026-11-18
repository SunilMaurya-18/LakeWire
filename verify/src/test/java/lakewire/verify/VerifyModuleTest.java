package lakewire.verify;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class VerifyModuleTest {

    @Test
    void moduleIsOnTheClasspath() {
        assertThat(VerifyModuleTest.class.getPackageName()).isEqualTo("lakewire.verify");
    }
}
