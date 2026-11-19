package lakewire.broker.api;

import java.util.List;
import lakewire.coordinator.LiveBroker;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ZoneMetadataTest {

    private static final List<LiveBroker> LIVE = List.of(
        new LiveBroker(3, "10.0.0.3", 9092, "c"),
        new LiveBroker(1, "10.0.0.1", 9092, "a"),
        new LiveBroker(2, "10.0.0.2", 9092, "b"),
        new LiveBroker(4, "10.0.0.4", 9092, "b")
    );

    @Test
    void zoneIsTheAzSuffix() {
        assertThat(ZoneRouting.clientZone(null)).isNull();
        assertThat(ZoneRouting.clientZone("plain")).isNull();
        assertThat(ZoneRouting.clientZone("az=a")).isNull();
        assertThat(ZoneRouting.clientZone("x,az=b")).isEqualTo("b");
        assertThat(ZoneRouting.clientZone("x;az=a")).isEqualTo("a");
        assertThat(ZoneRouting.clientZone("x,az=a;az=c")).isEqualTo("c");
        assertThat(ZoneRouting.clientZone("x,az=")).isNull();
        assertThat(ZoneRouting.clientZone("x,az=a,extra")).isNull();
    }

    @Test
    void leaderStaysInTheClientZoneAndIsStable() {
        assertThat(ZoneRouting.leader(LIVE, "orders", 0, "b"))
            .isEqualTo(ZoneRouting.leader(LIVE, "orders", 0, "b"))
            .isIn(2, 4);
        assertThat(ZoneRouting.leader(LIVE, "orders", 1, "b")).isIn(2, 4);
        assertThat(ZoneRouting.leader(LIVE, "orders", 0, "a")).isEqualTo(1);
        assertThat(ZoneRouting.leader(LIVE, "orders", 0, "missing"))
            .isEqualTo(ZoneRouting.leader(LIVE, "orders", 0, null));
        assertThat(ZoneRouting.leader(List.of(), "orders", 0, "a")).isEqualTo(-1);

        List<LiveBroker> reversed = List.of(LIVE.get(3), LIVE.get(2), LIVE.get(1), LIVE.get(0));
        assertThat(ZoneRouting.leader(reversed, "orders", 0, "b"))
            .isEqualTo(ZoneRouting.leader(LIVE, "orders", 0, "b"));
    }
}
