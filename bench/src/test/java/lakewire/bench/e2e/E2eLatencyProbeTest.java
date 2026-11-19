package lakewire.bench.e2e;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class E2eLatencyProbeTest {

    @Test
    void csvHeaderIsLaptopAndIncludesHardware() {
        List<Long> produce = new ArrayList<>(List.of(10L, 20L, 30L, 40L, 50L));
        List<Long> e2e = new ArrayList<>(List.of(100L, 200L, 300L, 400L, 1500L));
        String csv = E2eLatencyProbe.csv("127.0.0.1:9092", "bench", 5, produce, e2e, 0);
        assertThat(csv).contains("# label=laptop");
        assertThat(csv).contains("kafka-clients=4.4.0");
        assertThat(csv).contains("hardware=");
        assertThat(csv).contains("produce_ack");
        assertThat(csv).contains("e2e_consume");
    }
}
