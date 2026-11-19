package lakewire.verify;

import java.time.Duration;
import lakewire.verify.nemesis.NemesisKind;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class VerifyArgsTest {

    @Test
    void parsesTheCliShapeFromThePrd() {
        VerifyArgs args = VerifyArgs.parse(new String[] {
            "--nemesis", "kill,pause,partition-pg",
            "--duration", "10m",
            "--seed", "42"
        });
        assertThat(args.nemeses()).containsExactly(
            NemesisKind.KILL, NemesisKind.PAUSE, NemesisKind.PARTITION_PG);
        assertThat(args.duration()).isEqualTo(Duration.ofMinutes(10));
        assertThat(args.seed()).isEqualTo(42L);
    }
}
