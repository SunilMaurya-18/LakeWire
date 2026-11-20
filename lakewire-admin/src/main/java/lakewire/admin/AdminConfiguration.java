package lakewire.admin;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import lakewire.coordinator.AdminCatalog;
import lakewire.coordinator.Coordinator;
import lakewire.coordinator.CoordinatorConfig;
import lakewire.coordinator.TopicDirectory;
import lakewire.storage.LocalFsObjectStore;
import lakewire.storage.ObjectStore;
import lakewire.storage.S3ObjectStore;
import lakewire.storage.S3StorageConfig;
import lakewire.workers.WorkerConfig;
import lakewire.workers.WorkerJobs;
import lakewire.workers.WorkerMetrics;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
class AdminConfiguration {

    @Bean(destroyMethod = "close")
    Coordinator coordinator(Environment environment) {
        CoordinatorConfig config = new CoordinatorConfig(
            required(environment, "LAKEWIRE_PG_URL"),
            required(environment, "LAKEWIRE_PG_USER"),
            required(environment, "LAKEWIRE_PG_PASSWORD"),
            false,
            CoordinatorConfig.DEFAULT_LEASE_MS,
            CoordinatorConfig.DEFAULT_HEARTBEAT_MS,
            CoordinatorConfig.DEFAULT_GROUP_COMMIT
        );
        Coordinator coordinator = new Coordinator(config);
        coordinator.validate();
        return coordinator;
    }

    @Bean
    AdminCatalog adminCatalog(Coordinator coordinator) {
        return new AdminCatalog(coordinator);
    }

    @Bean
    TopicDirectory topicDirectory(Coordinator coordinator) {
        return new TopicDirectory(coordinator);
    }

    @Bean(destroyMethod = "close")
    ObjectStore objectStore(Environment environment) {
        Map<String, String> env = overlay(environment);
        String mode = env.getOrDefault("LAKEWIRE_STORAGE", "fs").trim();
        if (mode.isBlank() || "fs".equals(mode)) {
            return new LocalFsObjectStore(Path.of(env.getOrDefault("LAKEWIRE_DATA_DIR", "data")));
        }
        if ("s3".equals(mode)) {
            return new S3ObjectStore(S3StorageConfig.fromEnvironment(env));
        }
        throw new IllegalArgumentException("LAKEWIRE_STORAGE must be fs or s3");
    }

    @Bean
    WorkerJobs workerJobs(Coordinator coordinator, ObjectStore store, Environment environment) {
        return new WorkerJobs(
            coordinator,
            store,
            WorkerConfig.fromEnvironment(overlay(environment)),
            new WorkerMetrics()
        );
    }

    private static Map<String, String> overlay(Environment environment) {
        Map<String, String> env = new HashMap<>(System.getenv());
        put(env, environment, "LAKEWIRE_STORAGE");
        put(env, environment, "LAKEWIRE_DATA_DIR");
        put(env, environment, "LAKEWIRE_S3_ENDPOINT");
        put(env, environment, "LAKEWIRE_S3_BUCKET");
        put(env, environment, "LAKEWIRE_S3_REGION");
        put(env, environment, "LAKEWIRE_S3_PATH_STYLE");
        put(env, environment, "LAKEWIRE_S3_CONDITIONAL_PUT");
        put(env, environment, "LAKEWIRE_S3_ACCESS_KEY");
        put(env, environment, "LAKEWIRE_S3_SECRET_KEY");
        put(env, environment, "LAKEWIRE_OBJECT_PREFIX");
        put(env, environment, "LAKEWIRE_ORPHAN_TTL_MS");
        put(env, environment, "LAKEWIRE_GC_PERIOD_MS");
        put(env, environment, "LAKEWIRE_AUDIT_HEAD_CAP");
        env.putIfAbsent("LAKEWIRE_STORAGE", "fs");
        env.putIfAbsent("LAKEWIRE_DATA_DIR", Path.of("data").toString());
        return env;
    }

    private static void put(Map<String, String> env, Environment environment, String key) {
        String value = environment.getProperty(key);
        if (value != null && !value.isBlank()) {
            env.put(key, value);
        }
    }

    private static String required(Environment environment, String key) {
        String value = environment.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(key + " must be set");
        }
        return value.trim();
    }
}
