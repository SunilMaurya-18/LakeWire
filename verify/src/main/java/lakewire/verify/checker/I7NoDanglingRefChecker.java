package lakewire.verify.checker;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lakewire.storage.ObjectNotFoundException;
import lakewire.storage.ObjectStore;

/** I7: every segment's object exists, and live_refs matches the segment count. */
public final class I7NoDanglingRefChecker {

    private I7NoDanglingRefChecker() {
    }

    public static void assertModel(java.util.Set<String> segmentKeys, java.util.Set<String> existingKeys) {
        for (String key : segmentKeys) {
            if (!existingKeys.contains(key)) {
                throw new AssertionError("I7 dangling ref: missing object " + key);
            }
        }
    }

    public static void assertHolds(ObjectStore store, String jdbcUrl, String user, String password)
        throws Exception {
        List<String> keys = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
             PreparedStatement statement = connection.prepareStatement("""
                 SELECT o.key
                 FROM segments s
                 JOIN objects o ON o.id = s.object_id
                 """);
             ResultSet rows = statement.executeQuery()) {
            while (rows.next()) {
                keys.add(rows.getString(1));
            }
        }
        Set<String> unique = new LinkedHashSet<>(keys);
        for (String key : unique) {
            try {
                store.head(key).get(30, TimeUnit.SECONDS);
            } catch (Exception failure) {
                Throwable cause = failure;
                while (cause.getCause() != null && cause.getCause() != cause) {
                    cause = cause.getCause();
                }
                if (cause instanceof ObjectNotFoundException) {
                    throw new AssertionError("I7 dangling ref: missing object " + key);
                }
                throw failure;
            }
        }
        try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
             PreparedStatement statement = connection.prepareStatement(
                 "SELECT object_key FROM sl_audit_refcounts()");
             ResultSet rows = statement.executeQuery()) {
            if (rows.next()) {
                throw new AssertionError("I7 live_refs mismatch for " + rows.getString(1));
            }
        }
    }
}
