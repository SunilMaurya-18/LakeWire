package lakewire.coordinator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public final class BrokerRegistry {

    private final Coordinator coordinator;

    public BrokerRegistry(Coordinator coordinator) {
        this.coordinator = coordinator;
    }

    public long leaseMs() {
        return coordinator.config().leaseMs();
    }

    public long heartbeatIntervalMs() {
        return coordinator.config().heartbeatMs();
    }

    public long register(String id, String zone, String endpoint) {
        String sql = "SELECT sl_register_broker(?, ?, ?, ?)";
        try (Connection connection = coordinator.dataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, id);
            statement.setString(2, zone);
            statement.setString(3, endpoint);
            statement.setLong(4, coordinator.config().leaseMs());
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    throw new CoordinatorException("sl_register_broker returned no epoch");
                }
                return rows.getLong(1);
            }
        } catch (SQLException failure) {
            throw new CoordinatorException("broker register failed", failure);
        }
    }

    public boolean heartbeat(String id, long epoch) {
        String sql = "SELECT sl_heartbeat(?, ?, ?)";
        try (Connection connection = coordinator.dataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, id);
            statement.setLong(2, epoch);
            statement.setLong(3, coordinator.config().leaseMs());
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    throw new CoordinatorException("sl_heartbeat returned no row");
                }
                return rows.getBoolean(1);
            }
        } catch (SQLException failure) {
            throw new CoordinatorException("broker heartbeat failed", failure);
        }
    }

    public void createUploadIntent(String key, String brokerId, long epoch) {
        String sql = "SELECT sl_create_upload_intent(?, ?, ?)";
        try (Connection connection = coordinator.dataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, key);
            statement.setString(2, brokerId);
            statement.setLong(3, epoch);
            statement.execute();
        } catch (SQLException failure) {
            throw new CoordinatorException("upload intent failed", failure);
        }
    }
}
