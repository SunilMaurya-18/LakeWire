package lakewire.workers;

import java.util.List;
import lakewire.coordinator.RefcountMismatch;

public record AuditSnapshot(
    List<RefcountMismatch> refcountMismatches,
    List<String> missingCommittedKeys,
    List<String> leakedKeys
) {

    public boolean ok() {
        return refcountMismatches.isEmpty() && missingCommittedKeys.isEmpty() && leakedKeys.isEmpty();
    }
}
