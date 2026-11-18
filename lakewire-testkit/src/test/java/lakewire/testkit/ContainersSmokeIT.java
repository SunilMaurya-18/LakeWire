package lakewire.testkit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.postgresql.PostgreSQLContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

class ContainersSmokeIT {

    private static final Duration START_TIMEOUT = Duration.ofMinutes(8);
    private static final String BUCKET = "lakewire-smoke";

    @Test
    void postgresAnswersAndSeaweedFsAcceptsABucket() throws Exception {
        requireDocker();
        PostgreSQLContainer postgres = LakeWireContainers.postgres();
        GenericContainer<?> seaweed = LakeWireContainers.seaweedFs();
        try {
            Startables.deepStart(java.util.List.of(postgres, seaweed)).get(START_TIMEOUT.toMinutes(), TimeUnit.MINUTES);
            assertThat(selectOne(postgres)).isEqualTo(1);
            assertThat(createAndListBucket(seaweed)).contains(BUCKET);
        } finally {
            stopQuietly(seaweed);
            stopQuietly(postgres);
        }
    }

    private static void requireDocker() {
        try {
            if (!DockerClientFactory.instance().isDockerAvailable()) {
                fail("Docker is required for ContainersSmokeIT but is not available. Start Docker Desktop and rerun.");
            }
        } catch (RuntimeException failure) {
            fail("Docker is required for ContainersSmokeIT but is not available. Start Docker Desktop and rerun.",
                failure);
        }
    }

    private static int selectOne(PostgreSQLContainer postgres) throws Exception {
        DriverManager.setLoginTimeout(30);
        try (Connection connection = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(30);
            try (ResultSet result = statement.executeQuery("SELECT 1")) {
                assertThat(result.next()).isTrue();
                return result.getInt(1);
            }
        }
    }

    private static java.util.List<String> createAndListBucket(GenericContainer<?> seaweed) {
        String endpoint = "http://" + seaweed.getHost() + ":" + seaweed.getMappedPort(LakeWireContainers.SEAWEEDFS_S3_PORT);
        try (S3Client s3 = S3Client.builder()
            .endpointOverride(URI.create(endpoint))
            .region(Region.US_EAST_1)
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("lakewire", "lakewire")))
            .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
            .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
            .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
            .httpClient(UrlConnectionHttpClient.builder()
                .socketTimeout(Duration.ofSeconds(20))
                .connectionTimeout(Duration.ofSeconds(10))
                .build())
            .build()) {
            s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
            return s3.listBuckets().buckets().stream().map(Bucket::name).toList();
        }
    }

    private static void stopQuietly(Startable container) {
        try {
            container.stop();
        } catch (RuntimeException ignored) {
            // The container may never have started; the original failure is the one that matters.
        }
    }
}
