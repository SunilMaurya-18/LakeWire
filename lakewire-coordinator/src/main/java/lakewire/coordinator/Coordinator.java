package lakewire.coordinator;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Coordinator implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Coordinator.class);
    private static final int POOL_SIZE = 16;
    private static final String CALL = """
        SELECT idx, base_offset, error_code, log_start_offset
        FROM %s(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """;

    private final CoordinatorConfig config;
    private final HikariDataSource dataSource;

    public Coordinator(CoordinatorConfig config) {
        this.config = config;
        HikariConfig pool = new HikariConfig();
        pool.setJdbcUrl(config.jdbcUrl());
        pool.setUsername(config.username());
        pool.setPassword(config.password());
        pool.setMaximumPoolSize(POOL_SIZE);
        pool.setMinimumIdle(1);
        pool.setPoolName("lakewire-coordinator");
        pool.setAutoCommit(true);
        this.dataSource = new HikariDataSource(pool);
    }

    public CoordinatorConfig config() {
        return config;
    }

    public DataSource dataSource() {
        return dataSource;
    }

    public void start() {
        if (!config.migrate()) {
            return;
        }
        try {
            flyway().migrate();
            log.info("coordinator schema is current");
        } catch (RuntimeException failure) {
            throw new CoordinatorException("coordinator migration failed", failure);
        }
    }

    /** Admin only. Brokers and {@code lakewire dev} call {@link #start()}. */
    public void validate() {
        try {
            flyway().validate();
        } catch (RuntimeException failure) {
            throw new CoordinatorException("coordinator schema validation failed", failure);
        }
    }

    public List<String> trackedKeys() {
        return queryTexts("SELECT key FROM objects UNION SELECT object_key FROM upload_intents");
    }

    public List<CommitResult> commit(CommitRequest request) {
        return call("sl_commit_object", request, true);
    }

    /** Day-3 lab baseline. Production code calls {@link #commit}. */
    public List<CommitResult> commitRowLoop(CommitRequest request) {
        return call("sl_commit_object_rowloop", request, true);
    }

    /**
     * Runs each request as its own statement and commits once.
     * A failure rolls the whole batch back.
     */
    public List<List<CommitResult>> commitGroup(List<CommitRequest> requests) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                List<List<CommitResult>> results = new ArrayList<>(requests.size());
                for (CommitRequest request : requests) {
                    results.add(execute(connection, "sl_commit_object", request));
                }
                connection.commit();
                return results;
            } catch (SQLException | RuntimeException failure) {
                rollback(connection);
                if (failure instanceof CoordinatorException coordinatorException) {
                    throw coordinatorException;
                }
                throw new CoordinatorException("group commit failed", failure);
            }
        } catch (SQLException failure) {
            throw new CoordinatorException("group commit failed", failure);
        }
    }

    public ProducerIdAllocation initProducerId(long producerId, short epoch) {
        String sql = "SELECT error_code, producer_id, producer_epoch FROM sl_init_producer_id(?, ?)";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, producerId);
            statement.setShort(2, epoch);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    throw new CoordinatorException("sl_init_producer_id returned no row");
                }
                return new ProducerIdAllocation(rows.getShort(1), rows.getLong(2), rows.getShort(3));
            }
        } catch (SQLException failure) {
            throw new CoordinatorException("init producer id failed", failure);
        }
    }

    public List<CommitResult> commitOn(Connection connection, CommitRequest request) {
        try {
            return execute(connection, "sl_commit_object", request);
        } catch (SQLException failure) {
            throw new CoordinatorException("commit failed", failure);
        }
    }

    public List<String> gcExpiredIntents(long ttlMs) {
        return queryTexts("SELECT sl_gc_expired_intents(?)", ttlMs);
    }

    public List<String> uncommittableKeys(List<String> keys) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement("SELECT sl_uncommittable_keys(?)")) {
            Array array = connection.createArrayOf("text", keys.toArray(String[]::new));
            try {
                statement.setArray(1, array);
                return readTexts(statement);
            } finally {
                array.free();
            }
        } catch (SQLException failure) {
            throw new CoordinatorException("uncommittable keys failed", failure);
        }
    }

    public void applyRetention(long graceMs) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement("SELECT sl_apply_retention(?)")) {
            statement.setLong(1, graceMs);
            statement.executeQuery().close();
        } catch (SQLException failure) {
            throw new CoordinatorException("apply retention failed", failure);
        }
    }

    public List<String> dueDeletePending() {
        return queryTexts("SELECT sl_due_delete_pending()");
    }

    public void forgetObject(String key) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement("SELECT sl_forget_object(?)")) {
            statement.setString(1, key);
            statement.executeQuery().close();
        } catch (SQLException failure) {
            throw new CoordinatorException("forget object failed", failure);
        }
    }

    public long expireProducerState(long ttlMs) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement("SELECT sl_expire_producer_state(?)")) {
            statement.setLong(1, ttlMs);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    return 0;
                }
                return rows.getLong(1);
            }
        } catch (SQLException failure) {
            throw new CoordinatorException("expire producer state failed", failure);
        }
    }

    public List<RefcountMismatch> auditRefcounts() {
        String sql = """
            SELECT object_id, object_key, live_refs, actual_refs, live_bytes, actual_bytes
            FROM sl_audit_refcounts()
            """;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet rows = statement.executeQuery()) {
            List<RefcountMismatch> mismatches = new ArrayList<>();
            while (rows.next()) {
                mismatches.add(new RefcountMismatch(
                    rows.getObject(1, UUID.class),
                    rows.getString(2),
                    rows.getInt(3),
                    rows.getInt(4),
                    rows.getLong(5),
                    rows.getLong(6)
                ));
            }
            return List.copyOf(mismatches);
        } catch (SQLException failure) {
            throw new CoordinatorException("audit refcounts failed", failure);
        }
    }

    public List<String> committedObjectKeys() {
        return queryTexts("SELECT sl_committed_object_keys()");
    }

    private Flyway flyway() {
        return Flyway.configure()
            .dataSource(dataSource)
            .locations("classpath:db/migration")
            .load();
    }

    private List<String> queryTexts(String sql, long... args) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                statement.setLong(i + 1, args[i]);
            }
            return readTexts(statement);
        } catch (SQLException failure) {
            throw new CoordinatorException("coordinator query failed", failure);
        }
    }

    private static List<String> readTexts(PreparedStatement statement) throws SQLException {
        try (ResultSet rows = statement.executeQuery()) {
            List<String> values = new ArrayList<>();
            while (rows.next()) {
                values.add(rows.getString(1));
            }
            return List.copyOf(values);
        }
    }

    @Override
    public void close() {
        dataSource.close();
    }

    private List<CommitResult> call(String function, CommitRequest request, boolean autoCommit) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(autoCommit);
            return execute(connection, function, request);
        } catch (SQLException failure) {
            throw new CoordinatorException("commit failed", failure);
        }
    }

    private static List<CommitResult> execute(Connection connection, String function, CommitRequest request)
        throws SQLException {
        List<CommitEntry> entries = request.entries();
        int n = entries.size();
        Long[] topicIds = new Long[n];
        Integer[] parts = new Integer[n];
        Long[] byteStarts = new Long[n];
        Integer[] byteLens = new Integer[n];
        Integer[] nRecords = new Integer[n];
        Short[] nBatches = new Short[n];
        Long[] maxTimestamps = new Long[n];
        Long[] producerIds = new Long[n];
        Short[] producerEpochs = new Short[n];
        Integer[] firstSeqs = new Integer[n];
        Integer[] lastSeqs = new Integer[n];
        for (int i = 0; i < n; i++) {
            CommitEntry entry = entries.get(i);
            topicIds[i] = entry.topicId();
            parts[i] = entry.part();
            byteStarts[i] = entry.byteStart();
            byteLens[i] = entry.byteLen();
            nRecords[i] = entry.nRecords();
            nBatches[i] = entry.nBatches();
            maxTimestamps[i] = entry.maxTimestamp();
            producerIds[i] = entry.producerId();
            producerEpochs[i] = entry.producerEpoch();
            firstSeqs[i] = entry.firstSeq();
            lastSeqs[i] = entry.lastSeq();
        }

        List<Array> arrays = new ArrayList<>(11);
        try (PreparedStatement statement = connection.prepareStatement(CALL.formatted(function))) {
            statement.setObject(1, request.objectId());
            statement.setString(2, request.key());
            statement.setLong(3, request.sizeBytes());
            statement.setString(4, request.brokerId());
            statement.setLong(5, request.brokerEpoch());
            statement.setArray(6, track(arrays, connection.createArrayOf("int8", topicIds)));
            statement.setArray(7, track(arrays, connection.createArrayOf("int4", parts)));
            statement.setArray(8, track(arrays, connection.createArrayOf("int8", byteStarts)));
            statement.setArray(9, track(arrays, connection.createArrayOf("int4", byteLens)));
            statement.setArray(10, track(arrays, connection.createArrayOf("int4", nRecords)));
            statement.setArray(11, track(arrays, connection.createArrayOf("int2", nBatches)));
            statement.setArray(12, track(arrays, connection.createArrayOf("int8", maxTimestamps)));
            statement.setArray(13, track(arrays, connection.createArrayOf("int8", producerIds)));
            statement.setArray(14, track(arrays, connection.createArrayOf("int2", producerEpochs)));
            statement.setArray(15, track(arrays, connection.createArrayOf("int4", firstSeqs)));
            statement.setArray(16, track(arrays, connection.createArrayOf("int4", lastSeqs)));
            List<CommitResult> results = new ArrayList<>(n);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    results.add(new CommitResult(
                        rows.getInt(1),
                        rows.getLong(2),
                        rows.getShort(3),
                        rows.getLong(4)
                    ));
                }
            }
            return List.copyOf(results);
        } finally {
            for (Array array : arrays) {
                array.free();
            }
        }
    }

    private static Array track(List<Array> arrays, Array array) {
        arrays.add(array);
        return array;
    }

    private static void rollback(Connection connection) {
        try {
            if (!connection.isClosed()) {
                connection.rollback();
            }
        } catch (SQLException failure) {
            log.warn("group commit rollback failed: {}", failure.toString());
        }
    }
}
