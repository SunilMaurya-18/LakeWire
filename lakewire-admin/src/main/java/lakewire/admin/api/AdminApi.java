package lakewire.admin.api;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lakewire.coordinator.AdminCatalog;
import lakewire.coordinator.SegmentRow;
import lakewire.coordinator.TopicDirectory;
import lakewire.coordinator.TopicMutation;
import lakewire.coordinator.TopicSummary;
import lakewire.cost.CostEstimate;
import lakewire.cost.CostModel;
import lakewire.cost.CostQuery;
import lakewire.workers.AuditSnapshot;
import lakewire.workers.WorkerJobs;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.protocol.Errors;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
class AdminApi {

    private final AdminCatalog catalog;
    private final TopicDirectory topics;
    private final WorkerJobs jobs;

    AdminApi(AdminCatalog catalog, TopicDirectory topics, WorkerJobs jobs) {
        this.catalog = catalog;
        this.topics = topics;
        this.jobs = jobs;
    }

    @GetMapping("/cluster")
    ClusterResponse cluster() {
        return new ClusterResponse(catalog.clusterId(), catalog.brokers());
    }

    @GetMapping("/topics")
    List<TopicSummary> topics() {
        return catalog.topics();
    }

    @PostMapping("/topics")
    ResponseEntity<?> create(@RequestBody CreateTopicRequest request) {
        if (request == null || request.name() == null || request.name().isBlank()) {
            return ResponseEntity.badRequest().body(new ErrorBody("invalid_request", "name is required"));
        }
        try {
            Topic.validate(request.name());
        } catch (InvalidTopicException invalid) {
            return ResponseEntity.badRequest().body(new ErrorBody("invalid_topic", "invalid topic name"));
        }
        int partitions = request.partitions() == null || request.partitions() == -1 ? 1 : request.partitions();
        if (partitions < 1) {
            return ResponseEntity.badRequest().body(new ErrorBody("invalid_partitions", "partitions must be at least 1"));
        }
        Long retention = retention(request.config());
        if (retention == null) {
            return ResponseEntity.badRequest().body(new ErrorBody("invalid_config", "unsupported topic config"));
        }
        TopicMutation mutation = topics.createTopic(request.name(), partitions, retention, false);
        if (mutation.errorCode() == Errors.TOPIC_ALREADY_EXISTS.code()) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(new ErrorBody("topic_already_exists", "topic already exists"));
        }
        if (mutation.errorCode() != Errors.NONE.code()) {
            return ResponseEntity.badRequest()
                .body(new ErrorBody("create_failed", Errors.forCode(mutation.errorCode()).message()));
        }
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
            "name", request.name(),
            "topicId", mutation.topicId(),
            "partitions", partitions
        ));
    }

    @DeleteMapping("/topics/{name}")
    ResponseEntity<?> delete(@PathVariable("name") String name) {
        short code = topics.deleteTopic(name);
        if (code == Errors.UNKNOWN_TOPIC_OR_PARTITION.code()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new ErrorBody("unknown_topic", "unknown topic"));
        }
        if (code != Errors.NONE.code()) {
            return ResponseEntity.badRequest()
                .body(new ErrorBody("delete_failed", Errors.forCode(code).message()));
        }
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/topics/{name}/partitions/{part}/segments")
    ResponseEntity<?> segments(@PathVariable("name") String name, @PathVariable("part") int part) {
        if (!catalog.topicExists(name)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new ErrorBody("unknown_topic", "unknown topic"));
        }
        List<SegmentRow> segments = catalog.segments(name, part);
        return ResponseEntity.ok(segments);
    }

    @GetMapping("/objects/{id}")
    ResponseEntity<?> object(@PathVariable("id") UUID id) {
        return catalog.object(id)
            .<ResponseEntity<?>>map(ResponseEntity::ok)
            .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new ErrorBody("unknown_object", "unknown object")));
    }

    @GetMapping("/cost")
    Map<String, Object> cost(
        @RequestParam("gib_per_day") BigDecimal gibPerDay,
        @RequestParam(value = "zones", defaultValue = "3") int zones,
        @RequestParam(value = "retention_days", defaultValue = "7") int retentionDays
    ) {
        CostQuery query = new CostQuery(gibPerDay, zones, CostQuery.DEFAULT_OBJECT_BYTES, retentionDays, 1);
        CostEstimate estimate = CostModel.defaults().estimate(query);
        return Map.of(
            "query", query,
            "estimate", estimate,
            "caveats", CostModel.CAVEATS
        );
    }

    @PostMapping("/verify/audit")
    AuditResponse audit() {
        AuditSnapshot snapshot = jobs.inspect();
        return new AuditResponse(
            snapshot.ok(),
            snapshot.refcountMismatches(),
            snapshot.missingCommittedKeys(),
            snapshot.leakedKeys()
        );
    }

    private static Long retention(Map<String, String> config) {
        long retention = TopicDirectory.DEFAULT_RETENTION_MS;
        if (config == null) {
            return retention;
        }
        for (Map.Entry<String, String> entry : config.entrySet()) {
            if ("retention.ms".equals(entry.getKey())) {
                try {
                    retention = Long.parseLong(entry.getValue());
                } catch (NumberFormatException invalid) {
                    return null;
                }
                if (retention < -1) {
                    return null;
                }
            } else if ("cleanup.policy".equals(entry.getKey())) {
                if (!"delete".equals(entry.getValue())) {
                    return null;
                }
            } else {
                return null;
            }
        }
        return retention;
    }
}
