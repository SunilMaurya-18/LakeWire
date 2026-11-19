package lakewire.workers;

import java.time.Duration;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

class AdvisoryLockIT {

    @Test
    @Timeout(120)
    void onlyOneWorkerIsActiveAndTheStandbyTakesOver() throws Exception {
        WorkerConfig config = new WorkerConfig(
            true, 60_000, 60_000, 60_000, 60_000, 60_000, 0,
            7L * 24 * 60 * 60 * 1000, 60_000, 60_000, 150, "");
        try (WorkerHarness harness = WorkerHarness.start(config)) {
            WorkerMetrics metrics = new WorkerMetrics();
            try (WorkerRuntime first = new WorkerRuntime(config, harness.coordinator, harness.store, metrics);
                 WorkerRuntime second = new WorkerRuntime(config, harness.coordinator, harness.store, metrics)) {
                first.start();
                second.start();
                Awaitility.await().atMost(Duration.ofSeconds(10)).until(() ->
                    first.isLeader() ^ second.isLeader());
                WorkerRuntime active = first.isLeader() ? first : second;
                WorkerRuntime standby = first.isLeader() ? second : first;
                assertThat(active.isLeader()).isTrue();
                assertThat(standby.isLeader()).isFalse();
                int pid = active.lockBackendPid();
                assertThat(pid).isPositive();
                harness.terminate(pid);
                Awaitility.await().atMost(Duration.ofSeconds(15)).until(standby::isLeader);
                assertThat(first.isLeader() && second.isLeader()).isFalse();
            }
        }
    }
}
