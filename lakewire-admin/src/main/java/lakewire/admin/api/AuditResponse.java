package lakewire.admin.api;

import java.util.List;
import lakewire.coordinator.RefcountMismatch;

public record AuditResponse(
    boolean ok,
    List<RefcountMismatch> refcountMismatches,
    List<String> missingCommittedKeys,
    List<String> leakedKeys
) {
}
