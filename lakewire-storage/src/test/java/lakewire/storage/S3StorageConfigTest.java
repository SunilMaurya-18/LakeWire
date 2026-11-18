package lakewire.storage;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3StorageConfigTest {

    @Test
    void readsDocumentedDefaults() {
        S3StorageConfig config = S3StorageConfig.fromEnvironment(java.util.Map.of("LAKEWIRE_S3_BUCKET", "lake"));
        assertThat(config.region()).isEqualTo("us-east-1");
        assertThat(config.pathStyle()).isTrue();
        assertThat(config.conditionalPut()).isTrue();
        assertThat(config.endpoint()).isNull();
        assertThat(config.accessKey()).isNull();
    }

    @Test
    void requiresABucket() {
        assertThatThrownBy(() -> S3StorageConfig.fromEnvironment(java.util.Map.of()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("LAKEWIRE_S3_BUCKET");
    }
}
