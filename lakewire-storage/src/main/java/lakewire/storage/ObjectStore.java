package lakewire.storage;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Create-only object storage. {@code put} fails when the key already exists.
 * Implementations complete the futures on their own threads.
 */
public interface ObjectStore extends AutoCloseable {

    CompletableFuture<Void> put(String key, byte[] body);

    CompletableFuture<byte[]> getRange(String key, long start, int length);

    CompletableFuture<ObjectHead> head(String key);

    CompletableFuture<Void> delete(String key);

    CompletableFuture<List<String>> list(String prefix);

    @Override
    void close();
}
