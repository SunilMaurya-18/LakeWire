package lakewire.storage;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class StorageModuleTest {

    @Test
    void moduleIsOnTheClasspath() {
        assertThat(StorageModuleTest.class.getPackageName()).isEqualTo("lakewire.storage");
    }
}
