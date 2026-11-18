package lakewire.storage.lkw1;

import java.util.List;

public record Lkw1Object(String brokerId, long brokerEpoch, long createdMs, List<Lkw1Chunk> chunks) {
}
