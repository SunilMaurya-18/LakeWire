package lakewire.testkit;

import java.time.Duration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.toxiproxy.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Factories for the pinned dev images. Callers start and stop the containers.
 */
public final class LakeWireContainers {

    public static final String POSTGRES_IMAGE = "postgres:18.7-alpine";
    public static final String SEAWEEDFS_IMAGE = "chrislusf/seaweedfs:4.54";
    public static final String SILO_IMAGE = "pgsty/silo:RELEASE.2026-11-12T00-00-00Z";
    public static final String TOXIPROXY_IMAGE = "ghcr.io/shopify/toxiproxy:2.13.0";

    /** SeaweedFS S3 gateway port inside the container. */
    public static final int SEAWEEDFS_S3_PORT = 8333;

    private LakeWireContainers() {
    }

    public static PostgreSQLContainer postgres() {
        return new PostgreSQLContainer(DockerImageName.parse(POSTGRES_IMAGE))
            .withDatabaseName("lakewire")
            .withUsername("lakewire")
            .withPassword("lakewire")
            .withUrlParam("connectTimeout", "30")
            .withUrlParam("socketTimeout", "30");
    }

    public static GenericContainer<?> seaweedFs() {
        return new GenericContainer<>(DockerImageName.parse(SEAWEEDFS_IMAGE))
            .withCommand("server", "-s3", "-dir=/data")
            .withExposedPorts(SEAWEEDFS_S3_PORT)
            .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)));
    }

    /**
     * Silo, the MinIO-compatible image. Testcontainers 2.0.5 allowlists only
     * {@code minio/minio}, so the image is declared as a compatible substitute (A25).
     */
    public static MinIOContainer silo() {
        DockerImageName image = DockerImageName.parse(SILO_IMAGE).asCompatibleSubstituteFor("minio/minio");
        return new MinIOContainer(image);
    }

    public static ToxiproxyContainer toxiproxy() {
        return new ToxiproxyContainer(DockerImageName.parse(TOXIPROXY_IMAGE));
    }
}
