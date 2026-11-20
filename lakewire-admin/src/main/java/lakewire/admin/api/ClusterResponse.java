package lakewire.admin.api;

import java.util.List;
import lakewire.coordinator.BrokerSnapshot;

public record ClusterResponse(String clusterId, List<BrokerSnapshot> brokers) {
}
