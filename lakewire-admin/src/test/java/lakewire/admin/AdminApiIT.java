package lakewire.admin;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import lakewire.coordinator.Coordinator;
import lakewire.coordinator.CoordinatorConfig;
import lakewire.testkit.LakeWireContainers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.postgresql.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class AdminApiIT {

    static PostgreSQLContainer postgres;
    static Path dataDir;

    @LocalServerPort
    int port;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) throws Exception {
        if (postgres == null) {
            try {
                DockerClientFactory.instance().client();
            } catch (RuntimeException failure) {
                throw new IllegalStateException(
                    "Docker is required for AdminApiIT. Start Docker Desktop and rerun.", failure);
            }
            postgres = LakeWireContainers.postgres();
            postgres.start();
            dataDir = Files.createTempDirectory("lakewire-admin-it");
            try (Coordinator coordinator = new Coordinator(CoordinatorConfig.of(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword()))) {
                coordinator.start();
            }
        }
        registry.add("LAKEWIRE_PG_URL", postgres::getJdbcUrl);
        registry.add("LAKEWIRE_PG_USER", postgres::getUsername);
        registry.add("LAKEWIRE_PG_PASSWORD", postgres::getPassword);
        registry.add("LAKEWIRE_STORAGE", () -> "fs");
        registry.add("LAKEWIRE_DATA_DIR", () -> dataDir.toString());
        registry.add("LAKEWIRE_MIGRATE", () -> "false");
    }

    @AfterAll
    static void stopPostgres() {
        if (postgres != null) {
            postgres.stop();
        }
    }

    @Test
    void clusterTopicsCostAuditAndPrometheus() throws Exception {
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        HttpResponse<String> cluster = get(client, "/api/v1/cluster");
        assertThat(cluster.statusCode()).isEqualTo(200);
        assertThat(cluster.body()).contains("clusterId").contains("brokers");

        HttpResponse<String> created = post(client, "/api/v1/topics",
            "{\"name\":\"orders\",\"partitions\":2,\"config\":{\"retention.ms\":\"86400000\"}}");
        assertThat(created.statusCode()).isEqualTo(201);
        assertThat(created.body()).contains("orders");

        HttpResponse<String> dup = post(client, "/api/v1/topics", "{\"name\":\"orders\",\"partitions\":1}");
        assertThat(dup.statusCode()).isEqualTo(409);

        HttpResponse<String> topics = get(client, "/api/v1/topics");
        assertThat(topics.statusCode()).isEqualTo(200);
        assertThat(topics.body()).contains("orders").contains("highWatermark");

        HttpResponse<String> segments = get(client, "/api/v1/topics/orders/partitions/0/segments");
        assertThat(segments.statusCode()).as(segments.body()).isEqualTo(200);
        assertThat(segments.body()).isEqualTo("[]");

        HttpResponse<String> missing = get(client, "/api/v1/objects/00000000-0000-0000-0000-000000000001");
        assertThat(missing.statusCode()).isEqualTo(404);

        HttpResponse<String> cost = get(client, "/api/v1/cost?gib_per_day=1&zones=3");
        assertThat(cost.statusCode()).isEqualTo(200);
        assertThat(cost.body()).contains("lakewireTransfer").contains("caveats");

        HttpResponse<String> audit = post(client, "/api/v1/verify/audit", "");
        assertThat(audit.statusCode()).isEqualTo(200);
        assertThat(audit.body()).contains("\"ok\":true");

        HttpResponse<String> deleted = client.send(
            HttpRequest.newBuilder(URI.create(base() + "/api/v1/topics/orders"))
                .timeout(Duration.ofSeconds(10))
                .DELETE()
                .build(),
            HttpResponse.BodyHandlers.ofString()
        );
        assertThat(deleted.statusCode()).isEqualTo(204);

        HttpResponse<String> prometheus = get(client, "/actuator/prometheus");
        assertThat(prometheus.statusCode()).isEqualTo(200);
        assertThat(prometheus.body()).contains("#");
    }

    private String base() {
        return "http://127.0.0.1:" + port;
    }

    private HttpResponse<String> get(HttpClient client, String path) throws Exception {
        return client.send(
            HttpRequest.newBuilder(URI.create(base() + path)).timeout(Duration.ofSeconds(10)).GET().build(),
            HttpResponse.BodyHandlers.ofString()
        );
    }

    private HttpResponse<String> post(HttpClient client, String path, String body) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(base() + path))
            .timeout(Duration.ofSeconds(10))
            .header("Content-Type", "application/json");
        if (body == null || body.isEmpty()) {
            builder.POST(HttpRequest.BodyPublishers.noBody());
        } else {
            builder.POST(HttpRequest.BodyPublishers.ofString(body));
        }
        return client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }
}
