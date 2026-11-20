package lakewire.admin.api;

import java.util.Map;

public record CreateTopicRequest(String name, Integer partitions, Map<String, String> config) {
}
