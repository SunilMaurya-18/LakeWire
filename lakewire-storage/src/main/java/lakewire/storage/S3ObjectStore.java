package lakewire.storage;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * S3-compatible store. The HTTP client is AWS CRT, not the SDK Netty client.
 * Checksums are {@code WHEN_REQUIRED} so SeaweedFS and Silo accept the calls.
 */
public final class S3ObjectStore implements ObjectStore {

    static final String CRC_METADATA = "crc32c";
    private static final int MAX_ATTEMPTS = 3;
    private static final Logger log = LoggerFactory.getLogger(S3ObjectStore.class);

    private final S3StorageConfig config;
    private final AwsCredentialsProvider credentials;
    private final S3AsyncClient client;
    private final boolean conditionalPutSupported;
    private final AtomicBoolean closed = new AtomicBoolean();

    public S3ObjectStore(S3StorageConfig config) {
        this.config = config;
        this.credentials = credentials(config);
        this.client = client(config, credentials);
        this.conditionalPutSupported = probe(config.conditionalPut());
    }

    public boolean conditionalPutSupported() {
        return conditionalPutSupported;
    }

    @Override
    public CompletableFuture<Void> put(String key, byte[] body) {
        ObjectKeys.require(key);
        byte[] copy = body.clone();
        long crc = Crc32c.of(copy);
        if (!conditionalPutSupported) {
            return head(key).handle((existing, error) -> {
                if (error == null) {
                    return CompletableFuture.<Void>failedFuture(new ObjectAlreadyExistsException(key));
                }
                Throwable cause = unwrap(error);
                if (cause instanceof ObjectNotFoundException) {
                    return putRaw(key, copy, crc, false, 1);
                }
                return CompletableFuture.<Void>failedFuture(cause);
            }).thenCompose(future -> future);
        }
        return putRaw(key, copy, crc, true, 1);
    }

    @Override
    public CompletableFuture<byte[]> getRange(String key, long start, int length) {
        ObjectKeys.require(key);
        if (start < 0 || length < 0) {
            throw new IllegalArgumentException("range must not be negative");
        }
        if (length == 0) {
            return head(key).thenApply(ignored -> new byte[0]);
        }
        String range = "bytes=" + start + "-" + (start + length - 1);
        return withRetry(() -> client.getObject(
            builder -> builder.bucket(config.bucket()).key(key).range(range),
            AsyncResponseTransformer.toBytes()
        ).thenApply(bytes -> {
            byte[] body = bytes.asByteArray();
            if (body.length != length) {
                throw new IllegalArgumentException("range is outside " + key);
            }
            return body;
        }), 1);
    }

    @Override
    public CompletableFuture<ObjectHead> head(String key) {
        ObjectKeys.require(key);
        return withRetry(() -> client.headObject(builder -> builder.bucket(config.bucket()).key(key))
            .thenApply(response -> new ObjectHead(
                response.contentLength(),
                crcOf(response),
                response.lastModified() == null ? Instant.EPOCH : response.lastModified()
            )), 1);
    }

    @Override
    public CompletableFuture<Void> delete(String key) {
        ObjectKeys.require(key);
        return head(key).thenCompose(ignored -> withRetry(
            () -> client.deleteObject(builder -> builder.bucket(config.bucket()).key(key)).thenApply(response -> null),
            1
        ));
    }

    @Override
    public CompletableFuture<List<String>> list(String prefix) {
        String normalized = prefix == null ? "" : prefix;
        return listFrom(normalized, null, new ArrayList<>());
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            client.close();
            if (credentials instanceof AutoCloseable closeable) {
                try {
                    closeable.close();
                } catch (Exception failure) {
                    log.warn("s3 credentials close failed: {}", failure.toString());
                }
            }
        }
    }

    private CompletableFuture<Void> putRaw(String key, byte[] body, long crc, boolean conditional, int attempt) {
        PutObjectRequest.Builder request = PutObjectRequest.builder()
            .bucket(config.bucket())
            .key(key)
            .contentLength((long) body.length)
            .metadata(Map.of(CRC_METADATA, Long.toUnsignedString(crc)));
        if (conditional) {
            request.ifNoneMatch("*");
        }
        return client.putObject(request.build(), AsyncRequestBody.fromBytes(body)).handle((response, error) -> {
            if (error == null) {
                return CompletableFuture.<Void>completedFuture(null);
            }
            Throwable cause = unwrap(error);
            if (cause instanceof S3Exception s3 && s3.statusCode() == 412) {
                if (attempt == 1) {
                    return CompletableFuture.<Void>failedFuture(new ObjectAlreadyExistsException(key, s3));
                }
                return matchesUploaded(key, body.length, crc)
                    .thenCompose(matched -> matched
                        ? CompletableFuture.<Void>completedFuture(null)
                        : CompletableFuture.failedFuture(new ObjectAlreadyExistsException(key, s3)));
            }
            if (retriable(cause) && attempt < MAX_ATTEMPTS) {
                return backoff(attempt).thenCompose(ignored -> putRaw(key, body, crc, conditional, attempt + 1));
            }
            return CompletableFuture.<Void>failedFuture(map(key, cause));
        }).thenCompose(future -> future);
    }

    private CompletableFuture<Boolean> matchesUploaded(String key, int size, long crc) {
        return client.headObject(builder -> builder.bucket(config.bucket()).key(key))
            .thenApply(response -> response.contentLength() == size && crcOf(response) == crc)
            .exceptionally(error -> false);
    }

    private CompletableFuture<List<String>> listFrom(String prefix, String token, List<String> keys) {
        return withRetry(() -> client.listObjectsV2(builder -> {
            builder.bucket(config.bucket()).prefix(prefix);
            if (token != null) {
                builder.continuationToken(token);
            }
        }), 1).thenCompose(response -> {
            if (response.contents() != null) {
                for (S3Object object : response.contents()) {
                    keys.add(object.key());
                }
            }
            if (Boolean.TRUE.equals(response.isTruncated()) && response.nextContinuationToken() != null) {
                return listFrom(prefix, response.nextContinuationToken(), keys);
            }
            keys.sort(String::compareTo);
            return CompletableFuture.completedFuture(List.copyOf(keys));
        });
    }

    private <T> CompletableFuture<T> withRetry(java.util.function.Supplier<CompletableFuture<T>> call, int attempt) {
        return call.get().exceptionallyCompose(error -> {
            Throwable cause = unwrap(error);
            if (retriable(cause) && attempt < MAX_ATTEMPTS) {
                return backoff(attempt).thenCompose(ignored -> withRetry(call, attempt + 1));
            }
            return CompletableFuture.failedFuture(map(null, cause));
        });
    }

    private boolean probe(boolean requested) {
        if (!requested) {
            log.info("s3 conditional put probe: skipped");
            return false;
        }
        String key = ".lakewire-probe/" + UUID.randomUUID();
        byte[] body = new byte[] {1, 2, 3, 4};
        long crc = Crc32c.of(body);
        try {
            putProbe(key, body, crc);
        } catch (CompletionException failure) {
            Throwable cause = unwrap(failure);
            if (rejected(cause)) {
                log.info("s3 conditional put probe: store rejected If-None-Match");
                return false;
            }
            log.warn("s3 conditional put probe failed; treating If-None-Match as unsupported: {}", cause.toString());
            return false;
        }
        boolean supported;
        try {
            putRaw(key, body, crc, true, 1).join();
            supported = false;
        } catch (CompletionException failure) {
            Throwable cause = unwrap(failure);
            if (cause instanceof ObjectAlreadyExistsException) {
                supported = true;
            } else if (rejected(cause)) {
                supported = false;
            } else {
                log.warn("s3 conditional put probe failed; treating If-None-Match as unsupported: {}", cause.toString());
                return false;
            }
        }
        delete(key).exceptionally(error -> {
            log.warn("s3 conditional put probe cleanup failed: {}", unwrap(error).toString());
            return null;
        }).join();
        log.info("s3 conditional put probe: supported={}", supported);
        return supported;
    }

    private void putProbe(String key, byte[] body, long crc) {
        CompletionException last = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                putRaw(key, body, crc, true, 1).join();
                return;
            } catch (CompletionException failure) {
                last = failure;
                Throwable cause = unwrap(failure);
                if (rejected(cause) || cause instanceof ObjectAlreadyExistsException) {
                    throw failure;
                }
                if (attempt == MAX_ATTEMPTS) {
                    throw failure;
                }
                try {
                    Thread.sleep(200L * attempt);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw failure;
                }
            }
        }
        throw last;
    }

    private static boolean rejected(Throwable cause) {
        return cause instanceof S3Exception s3 && (s3.statusCode() == 400 || s3.statusCode() == 501);
    }

    private static boolean retriable(Throwable cause) {
        if (cause instanceof S3Exception s3) {
            return s3.statusCode() >= 500 && s3.statusCode() <= 599;
        }
        return cause instanceof SdkClientException;
    }

    private static Throwable map(String key, Throwable cause) {
        if (cause instanceof ObjectNotFoundException
            || cause instanceof ObjectAlreadyExistsException
            || cause instanceof TransientStorageException
            || cause instanceof IllegalArgumentException) {
            return cause;
        }
        if (cause instanceof NoSuchKeyException missing) {
            return new ObjectNotFoundException(key == null ? missing.getMessage() : key, missing);
        }
        if (cause instanceof S3Exception s3) {
            int status = s3.statusCode();
            if (status == 404) {
                return new ObjectNotFoundException(key == null ? "unknown" : key, s3);
            }
            if (status == 412) {
                return new ObjectAlreadyExistsException(key == null ? "unknown" : key, s3);
            }
            if (status >= 500) {
                return new TransientStorageException("s3 status " + status, s3);
            }
            return new TransientStorageException("s3 status " + status, s3);
        }
        if (cause instanceof SdkClientException client) {
            return new TransientStorageException("s3 client failure", client);
        }
        return cause instanceof RuntimeException runtime ? runtime : new TransientStorageException("s3 failure", cause);
    }

    private static long crcOf(HeadObjectResponse response) {
        String raw = response.metadata().get(CRC_METADATA);
        if (raw == null || raw.isBlank()) {
            return -1L;
        }
        return Long.parseUnsignedLong(raw);
    }

    private static CompletableFuture<Void> backoff(int attempt) {
        long delay = 20L * attempt + ThreadLocalRandom.current().nextInt(21);
        return CompletableFuture.runAsync(
            () -> { },
            CompletableFuture.delayedExecutor(delay, TimeUnit.MILLISECONDS)
        );
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException || current instanceof java.util.concurrent.ExecutionException)
            && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static AwsCredentialsProvider credentials(S3StorageConfig config) {
        if (config.accessKey() != null) {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(config.accessKey(), config.secretKey()));
        }
        return DefaultCredentialsProvider.builder().build();
    }

    private static S3AsyncClient client(S3StorageConfig config, AwsCredentialsProvider credentials) {
        var builder = S3AsyncClient.builder()
            .httpClientBuilder(AwsCrtAsyncHttpClient.builder()
                .connectionTimeout(Duration.ofSeconds(10)))
            .region(Region.of(config.region()))
            .credentialsProvider(credentials)
            .serviceConfiguration(software.amazon.awssdk.services.s3.S3Configuration.builder()
                .pathStyleAccessEnabled(config.pathStyle())
                .build())
            .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
            .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED);
        if (config.endpoint() != null) {
            builder.endpointOverride(config.endpoint());
        }
        return builder.build();
    }
}
