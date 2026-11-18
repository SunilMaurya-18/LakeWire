package lakewire.storage;

import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class UuidV7Test {

    @Test
    void versionsAndMonotonicity() {
        UUID previous = UuidV7.next();
        assertThat(previous.version()).isEqualTo(7);
        assertThat(previous.variant()).isEqualTo(2);
        for (int i = 0; i < 5_000; i++) {
            UUID next = UuidV7.next();
            assertThat(next.version()).isEqualTo(7);
            assertThat(next.variant()).isEqualTo(2);
            if (UuidV7.unixMillis(next) == UuidV7.unixMillis(previous)) {
                assertThat(UuidV7.sequence(next)).isGreaterThan(UuidV7.sequence(previous));
            } else {
                assertThat(UuidV7.unixMillis(next)).isGreaterThan(UuidV7.unixMillis(previous));
            }
            previous = next;
        }
    }
}
