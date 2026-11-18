package lakewire.storage;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ObjectKeysTest {

    @Test
    void buildsTheUtcLayout() {
        UUID id = UUID.fromString("018f6c8e-7b2a-7000-8000-000000000001");
        String key = ObjectKeys.objectKey(
            "lake",
            "broker-a",
            3,
            id,
            Instant.parse("2026-11-18T09:04:05Z")
        );
        assertThat(key).isEqualTo("lake/v2/2026/11/18/09/broker-a-3-" + id + ".lkw");
    }

    @Test
    void omitsABlankPrefix() {
        String key = ObjectKeys.objectKey("", "b", 0, new UUID(0, 1), Instant.EPOCH);
        assertThat(key).startsWith("v2/1970/01/01/00/b-0-");
    }

    @Test
    void rejectsTraversalAndSeparators() {
        assertThatThrownBy(() -> ObjectKeys.objectKey("..", "b", 0, new UUID(0, 1), Instant.EPOCH))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ObjectKeys.objectKey("ok", "a/b", 0, new UUID(0, 1), Instant.EPOCH))
            .isInstanceOf(IllegalArgumentException.class);
    }
}
