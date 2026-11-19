package lakewire.verify;

import java.time.Duration;
import java.util.List;
import lakewire.verify.nemesis.NemesisKind;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SchedulerTest {

    @Test
    void sameSeedReplaysTheSamePlan() {
        List<NemesisKind> kinds = List.of(NemesisKind.KILL, NemesisKind.PAUSE, NemesisKind.PARTITION_PG);
        List<Scheduler.Event> a = Scheduler.plan(42, Duration.ofMinutes(3), kinds, 3);
        List<Scheduler.Event> b = Scheduler.plan(42, Duration.ofMinutes(3), kinds, 3);
        assertThat(a).isNotEmpty().isEqualTo(b);
        assertThat(Scheduler.plan(43, Duration.ofMinutes(3), kinds, 3)).isNotEqualTo(a);
    }
}
