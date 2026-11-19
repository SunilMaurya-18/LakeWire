package lakewire.storage;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

/** Create-only local store. The object appears via an atomic move in the same directory. */
public final class LocalFsObjectStore implements ObjectStore {

    private final Path root;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicBoolean closed = new AtomicBoolean();

    public LocalFsObjectStore(Path root) {
        if (root == null) {
            throw new IllegalArgumentException("root must not be null");
        }
        this.root = root;
    }

    @Override
    public CompletableFuture<Void> put(String key, byte[] body) {
        ObjectKeys.require(key);
        byte[] copy = copy(body);
        return CompletableFuture.runAsync(() -> putSync(key, copy), executor);
    }

    @Override
    public CompletableFuture<byte[]> getRange(String key, long start, int length) {
        ObjectKeys.require(key);
        requireRange(start, length);
        return CompletableFuture.supplyAsync(() -> readSync(key, start, length), executor);
    }

    @Override
    public CompletableFuture<ObjectHead> head(String key) {
        ObjectKeys.require(key);
        return CompletableFuture.supplyAsync(() -> headSync(key), executor);
    }

    @Override
    public CompletableFuture<Void> delete(String key) {
        ObjectKeys.require(key);
        return CompletableFuture.runAsync(() -> deleteSync(key), executor);
    }

    @Override
    public CompletableFuture<List<String>> list(String prefix) {
        String normalized = prefix == null ? "" : prefix;
        return CompletableFuture.supplyAsync(() -> listSync(normalized), executor);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            executor.close();
        }
    }

    private void putSync(String key, byte[] body) {
        Path target = resolve(key);
        // One store lock makes the exists-check and the move atomic in this process.
        // Windows ATOMIC_MOVE replaces an existing file (A32).
        synchronized (this) {
            try {
                Files.createDirectories(target.getParent());
                if (Files.exists(target)) {
                    throw new ObjectAlreadyExistsException(key);
                }
                Path temp = Files.createTempFile(target.getParent(), ".put-", ".tmp");
                try {
                    Files.write(temp, body);
                    try {
                        Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE);
                    } catch (FileAlreadyExistsException exists) {
                        throw new ObjectAlreadyExistsException(key, exists);
                    }
                } finally {
                    Files.deleteIfExists(temp);
                }
            } catch (ObjectAlreadyExistsException exists) {
                throw exists;
            } catch (IOException failure) {
                throw new TransientStorageException("local put failed for " + key, failure);
            }
        }
    }

    private byte[] readSync(String key, long start, int length) {
        Path path = existing(key);
        try {
            long size = Files.size(path);
            if (start > size || start + length > size) {
                throw new IllegalArgumentException("range is outside " + key);
            }
            if (length == 0) {
                return new byte[0];
            }
            byte[] all = Files.readAllBytes(path);
            byte[] slice = new byte[length];
            System.arraycopy(all, (int) start, slice, 0, length);
            return slice;
        } catch (IOException failure) {
            throw new TransientStorageException("local read failed for " + key, failure);
        }
    }

    private ObjectHead headSync(String key) {
        Path path = existing(key);
        try {
            byte[] body = Files.readAllBytes(path);
            return new ObjectHead(body.length, Crc32c.of(body), Files.getLastModifiedTime(path).toInstant());
        } catch (IOException failure) {
            throw new TransientStorageException("local head failed for " + key, failure);
        }
    }

    private void deleteSync(String key) {
        Path path = existing(key);
        try {
            Files.delete(path);
        } catch (IOException failure) {
            throw new TransientStorageException("local delete failed for " + key, failure);
        }
    }

    private List<String> listSync(String prefix) {
        if (!Files.exists(root)) {
            return List.of();
        }
        try (Stream<Path> walk = Files.walk(root)) {
            List<String> keys = new ArrayList<>();
            for (Path path : walk.filter(Files::isRegularFile).toList()) {
                String name = path.getFileName().toString();
                if (name.startsWith(".put-")) {
                    continue;
                }
                String key = root.relativize(path).toString().replace('\\', '/');
                if (key.startsWith(prefix)) {
                    keys.add(key);
                }
            }
            keys.sort(Comparator.naturalOrder());
            return List.copyOf(keys);
        } catch (IOException failure) {
            throw new TransientStorageException("local list failed", failure);
        }
    }

    private Path existing(String key) {
        Path path = resolve(key);
        if (!Files.isRegularFile(path)) {
            throw new ObjectNotFoundException(key);
        }
        return path;
    }

    private Path resolve(String key) {
        Path resolved = root.resolve(key).normalize();
        if (!resolved.startsWith(root.normalize())) {
            throw new IllegalArgumentException("invalid object key");
        }
        return resolved;
    }

    private static void requireRange(long start, int length) {
        if (start < 0 || length < 0) {
            throw new IllegalArgumentException("range must not be negative");
        }
    }

    private static byte[] copy(byte[] body) {
        if (body == null) {
            throw new IllegalArgumentException("body must not be null");
        }
        return body.clone();
    }
}
