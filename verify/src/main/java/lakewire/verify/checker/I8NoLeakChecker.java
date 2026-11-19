package lakewire.verify.checker;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lakewire.storage.ObjectHead;
import lakewire.storage.ObjectStore;

/**
 * I8: a bucket key without an {@code objects} or {@code upload_intents} row is
 * younger than orphan TTL + GC period (A45).
 */
public final class I8NoLeakChecker {

    private I8NoLeakChecker() {
    }

    public static void assertModel(Instant lastModified, Instant now, Duration maxAge, String key) {
        Duration age = Duration.between(lastModified, now);
        if (age.compareTo(maxAge) >= 0) {
            throw new AssertionError("I8 leak: " + key + " age " + age + " >= " + maxAge);
        }
    }

    public static void assertHolds(
        ObjectStore store,
        String jdbcUrl,
        String user,
        String password,
        String prefix,
        Duration orphanTtl,
        Duration gcPeriod
    ) throws Exception {
        Set<String> known = new HashSet<>();
        try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
             PreparedStatement statement = connection.prepareStatement("""
                 SELECT key FROM objects
                 UNION
                 SELECT object_key FROM upload_intents
                 """);
             ResultSet rows = statement.executeQuery()) {
            while (rows.next()) {
                known.add(rows.getString(1));
            }
        }
        List<String> listed = store.list(prefix == null ? "" : prefix).get(30, TimeUnit.SECONDS);
        Duration maxAge = orphanTtl.plus(gcPeriod);
        Instant now = Instant.now();
        for (String key : listed) {
            if (known.contains(key)) {
                continue;
            }
            ObjectHead head = store.head(key).get(30, TimeUnit.SECONDS);
            Duration age = Duration.between(head.lastModified(), now);
            if (age.compareTo(maxAge) >= 0) {
                throw new AssertionError("I8 leak: " + key + " age " + age + " >= " + maxAge);
            }
        }
    }
}
