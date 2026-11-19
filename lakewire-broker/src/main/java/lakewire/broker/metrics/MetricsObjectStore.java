package lakewire.broker.metrics;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import lakewire.storage.ObjectHead;
import lakewire.storage.ObjectNotFoundException;
import lakewire.storage.ObjectStore;

/** Counts PUT/GET outcomes for {@code lakewire_s3_*_total}. */
public final class MetricsObjectStore implements ObjectStore {

    private final ObjectStore inner;
    private final LakeWireMetrics metrics;

    public MetricsObjectStore(ObjectStore inner, LakeWireMetrics metrics) {
        this.inner = inner;
        this.metrics = metrics;
    }

    @Override
    public CompletableFuture<Void> put(String key, byte[] body) {
        return inner.put(key, body).whenComplete((ignored, error) ->
            metrics.s3Put(error == null ? "ok" : "error"));
    }

    @Override
    public CompletableFuture<byte[]> getRange(String key, long start, int length) {
        return inner.getRange(key, start, length).whenComplete((ignored, error) -> {
            if (error == null) {
                metrics.s3Get("ok");
            } else if (root(error) instanceof ObjectNotFoundException) {
                metrics.s3Get("not_found");
            } else {
                metrics.s3Get("error");
            }
        });
    }

    @Override
    public CompletableFuture<ObjectHead> head(String key) {
        return inner.head(key);
    }

    @Override
    public CompletableFuture<Void> delete(String key) {
        return inner.delete(key);
    }

    @Override
    public CompletableFuture<List<String>> list(String prefix) {
        return inner.list(prefix);
    }

    @Override
    public void close() {
        inner.close();
    }

    private static Throwable root(Throwable error) {
        Throwable current = error;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }
}
