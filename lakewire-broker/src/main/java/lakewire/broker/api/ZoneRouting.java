package lakewire.broker.api;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import lakewire.coordinator.LiveBroker;

/** Zone hint from {@code client.id}, and the broker metadata should name as leader. */
public final class ZoneRouting {

    private ZoneRouting() {
    }

    /** {@code ,az=} or {@code ;az=} at the end of {@code client.id}. Anything else is no zone (A16). */
    public static String clientZone(String clientId) {
        if (clientId == null) {
            return null;
        }
        int at = Math.max(clientId.lastIndexOf(",az="), clientId.lastIndexOf(";az="));
        if (at < 0) {
            return null;
        }
        String zone = clientId.substring(at + 4);
        if (zone.isEmpty() || zone.indexOf(',') >= 0 || zone.indexOf(';') >= 0) {
            return null;
        }
        return zone;
    }

    /**
     * Among live brokers in {@code zone}, sorted by node id, pick {@code floorMod(hash, n)}.
     * No zone, or nobody in it, uses every live broker. Nobody live returns -1.
     */
    public static int leader(List<LiveBroker> live, String topic, int partition, String zone) {
        List<LiveBroker> pool = new ArrayList<>();
        if (zone != null) {
            for (LiveBroker broker : live) {
                if (zone.equals(broker.zone())) {
                    pool.add(broker);
                }
            }
        }
        if (pool.isEmpty()) {
            pool.addAll(live);
        }
        if (pool.isEmpty()) {
            return -1;
        }
        pool.sort(Comparator.comparingInt(LiveBroker::nodeId));
        return pool.get(Math.floorMod(mix(topic, partition), pool.size())).nodeId();
    }

    static int mix(String topic, int partition) {
        int hash = topic == null ? 0 : topic.hashCode();
        return 31 * hash + partition;
    }
}
