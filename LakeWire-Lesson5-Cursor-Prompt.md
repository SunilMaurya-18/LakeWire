# Cursor prompt: apply LakeWire Lesson 5

Paste everything below the line into Cursor's agent, with the project root (`D:\lakewire`, or `D:\lakewire-broker` if you didn't rename it) open as the workspace.

---

Apply "LakeWire Lesson 5" to this Maven multi-module project (root artifact `lakewire-parent`, modules `lakewire-protocol` and `lakewire-broker`, Java 21, no Spring). Follow these steps exactly and do nothing else.

RULES
- Only touch the files listed below. Do not edit, create, rename or reformat any other file (in particular: do NOT touch the root pom.xml, lakewire-broker/pom.xml, BrokerConfig.java, LakeWireBroker.java, logback.xml, deploy/, .gitignore).
- Do NOT create .mvn/jvm.config or any other config file, and do not add JVM flags.
- Write each file with EXACTLY the content given: same package names (`lakewire.protocol`, `lakewire.broker...`; there is no `dev.` prefix), same code, no extra comments, no "improvements", no reformatting.
- If a step fails, report the exact error output and stop. Do not redesign the code to make it pass.

STEP 1: Delete these files (they are replaced by kafka-clients classes):
- `lakewire-protocol/src/main/java/lakewire/protocol/ApiKey.java`
- `lakewire-protocol/src/main/java/lakewire/protocol/ErrorCode.java`
- `lakewire-protocol/src/main/java/lakewire/protocol/RequestHeader.java`
- `lakewire-protocol/src/main/java/lakewire/protocol/ByteBufUtils.java`
- `lakewire-protocol/src/main/java/lakewire/protocol/ApiVersionsHandler.java`
- `lakewire-protocol/src/main/java/lakewire/protocol/InvalidRequestException.java`
Also delete `.mvn/jvm.config` if it exists. Do not delete anything else in `.mvn/`.

STEP 2: Create or overwrite these files with exactly this content (paths are relative to the project root; create missing folders such as `lakewire-broker/src/main/java/lakewire/broker/api/` and `lakewire-protocol/src/test/java/lakewire/protocol/`):

FILE 1/15: `lakewire-protocol/pom.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>lakewire</groupId>
        <artifactId>lakewire-parent</artifactId>
        <version>0.1.0-SNAPSHOT</version>
    </parent>

    <artifactId>lakewire-protocol</artifactId>
    <name>LakeWire Protocol</name>
    <description>Kafka wire-protocol codec glue for LakeWire</description>

    <dependencies>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
        </dependency>

        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

FILE 2/15: `lakewire-protocol/src/main/java/lakewire/protocol/SupportedApi.java`

```java
package lakewire.protocol;

import java.util.Optional;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionCollection;
import org.apache.kafka.common.protocol.ApiKeys;

/**
 * The single registry of Kafka APIs that LakeWire implements, with the version range we answer.
 * ApiVersions advertises exactly this list (PRD §10.1), so add an entry here only when its handler exists.
 */
public enum SupportedApi {
    API_VERSIONS(ApiKeys.API_VERSIONS, (short) 0, (short) 3);

    private final ApiKeys apiKey;
    private final short minVersion;
    private final short maxVersion;

    SupportedApi(ApiKeys apiKey, short minVersion, short maxVersion) {
        this.apiKey = apiKey;
        this.minVersion = minVersion;
        this.maxVersion = maxVersion;
    }

    public ApiKeys apiKey() {
        return apiKey;
    }

    public short minVersion() {
        return minVersion;
    }

    public short maxVersion() {
        return maxVersion;
    }

    public boolean supportsVersion(short version) {
        return version >= minVersion && version <= maxVersion;
    }

    public static Optional<SupportedApi> forApiKey(ApiKeys apiKey) {
        for (SupportedApi api : values()) {
            if (api.apiKey == apiKey) {
                return Optional.of(api);
            }
        }
        return Optional.empty();
    }

    /** Builds a fresh collection each call: Kafka's collection links its elements, so they can't be shared. */
    public static ApiVersionCollection advertisedApiVersions() {
        ApiVersionCollection collection = new ApiVersionCollection();
        for (SupportedApi api : values()) {
            collection.add(new ApiVersion()
                .setApiKey(api.apiKey.id)
                .setMinVersion(api.minVersion)
                .setMaxVersion(api.maxVersion));
        }
        return collection;
    }
}
```

FILE 3/15: `lakewire-protocol/src/main/java/lakewire/protocol/KafkaRequest.java`

```java
package lakewire.protocol;

import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.RequestHeader;

/** One decoded request: Kafka's header plus Kafka's typed request body. */
public record KafkaRequest(RequestHeader header, AbstractRequest body) {
}
```

FILE 4/15: `lakewire-protocol/src/main/java/lakewire/protocol/KafkaCodec.java`

```java
package lakewire.protocol;

import java.nio.ByteBuffer;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;

/** Glue between raw Kafka frames and kafka-clients' generated message classes. */
public final class KafkaCodec {

    private KafkaCodec() {
    }

    /**
     * Decodes one frame (without its 4-byte length prefix).
     *
     * @throws org.apache.kafka.common.errors.InvalidRequestException for an unknown api_key or a malformed header
     */
    public static KafkaRequest decodeRequest(ByteBuffer frame) {
        RequestHeader header = RequestHeader.parse(frame);
        if (header.apiKey() == ApiKeys.API_VERSIONS && !header.isApiVersionSupported()) {
            // A client newer than this kafka-clients jar: don't parse a body we can't read.
            // Treat it as v0 and remember the real version, as Kafka's own broker does.
            ApiVersionsRequest body =
                new ApiVersionsRequest(new ApiVersionsRequestData(), (short) 0, header.apiVersion());
            return new KafkaRequest(header, body);
        }
        AbstractRequest body =
            AbstractRequest.parseRequest(header.apiKey(), header.apiVersion(), new ByteBufferAccessor(frame)).request;
        return new KafkaRequest(header, body);
    }

    /** Encodes response header + body (without the length prefix, which Netty's LengthFieldPrepender adds). */
    public static ByteBuffer encodeResponse(RequestHeader requestHeader, short responseVersion,
                                            AbstractResponse response) {
        ResponseHeader responseHeader = requestHeader.toResponseHeader();
        ResponseHeaderData headerData = responseHeader.data();
        short headerVersion = responseHeader.headerVersion();
        ApiMessage body = response.data();

        ObjectSerializationCache cache = new ObjectSerializationCache();
        int size = headerData.size(cache, headerVersion) + body.size(cache, responseVersion);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        ByteBufferAccessor writer = new ByteBufferAccessor(buffer);
        headerData.write(writer, cache, headerVersion);
        body.write(writer, cache, responseVersion);
        buffer.flip();
        return buffer;
    }
}
```

FILE 5/15: `lakewire-broker/src/main/java/lakewire/broker/api/RequestProcessor.java`

```java
package lakewire.broker.api;

import java.nio.ByteBuffer;

/** Turns one request frame into one response frame. Runs on a virtual thread, so it may block. */
@FunctionalInterface
public interface RequestProcessor {

    /**
     * @param request the frame without its length prefix
     * @return the response (header + body) without its length prefix
     * @throws RuntimeException if the request is unreadable; the connection is then closed
     */
    ByteBuffer process(ByteBuffer request);
}
```

FILE 6/15: `lakewire-broker/src/main/java/lakewire/broker/api/ApiVersionsHandler.java`

```java
package lakewire.broker.api;

import java.nio.ByteBuffer;
import lakewire.protocol.KafkaCodec;
import lakewire.protocol.KafkaRequest;
import lakewire.protocol.SupportedApi;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.RequestHeader;

/** ApiVersions (key 18): tells the client which APIs and versions LakeWire speaks. */
public final class ApiVersionsHandler {

    private ApiVersionsHandler() {
    }

    public static ByteBuffer handle(KafkaRequest request) {
        RequestHeader header = request.header();
        ApiVersionsRequest body = (ApiVersionsRequest) request.body();

        if (body.hasUnsupportedRequestVersion() || !SupportedApi.API_VERSIONS.supportsVersion(header.apiVersion())) {
            // KIP-511: answer with a v0 body, UNSUPPORTED_VERSION and our list, so the client retries
            // with the highest version we support.
            ApiVersionsResponseData data = new ApiVersionsResponseData()
                .setErrorCode(Errors.UNSUPPORTED_VERSION.code())
                .setApiKeys(SupportedApi.advertisedApiVersions());
            return KafkaCodec.encodeResponse(header, (short) 0, new ApiVersionsResponse(data));
        }

        Errors error = body.isValid() ? Errors.NONE : Errors.INVALID_REQUEST;
        ApiVersionsResponseData data = new ApiVersionsResponseData()
            .setErrorCode(error.code())
            .setApiKeys(SupportedApi.advertisedApiVersions());
        return KafkaCodec.encodeResponse(header, header.apiVersion(), new ApiVersionsResponse(data));
    }
}
```

FILE 7/15: `lakewire-broker/src/main/java/lakewire/broker/api/RequestRouter.java`

```java
package lakewire.broker.api;

import java.nio.ByteBuffer;
import lakewire.protocol.KafkaCodec;
import lakewire.protocol.KafkaRequest;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Decodes a frame with Kafka's classes and sends it to the handler for its API. */
public final class RequestRouter implements RequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(RequestRouter.class);

    @Override
    public ByteBuffer process(ByteBuffer frame) {
        KafkaRequest request = KafkaCodec.decodeRequest(frame);
        RequestHeader header = request.header();
        log.debug("{} v{} correlationId={} clientId={}",
            header.apiKey(), header.apiVersion(), header.correlationId(), header.clientId());

        // Keep this switch in step with SupportedApi: every entry there needs a case here.
        return switch (header.apiKey()) {
            case API_VERSIONS -> ApiVersionsHandler.handle(request);
            default -> unsupported(request);
        };
    }

    /** A real Kafka API that we don't implement yet: answer with Kafka's own error shape for it. */
    private static ByteBuffer unsupported(KafkaRequest request) {
        AbstractResponse response = request.body().getErrorResponse(Errors.UNSUPPORTED_VERSION.exception());
        return KafkaCodec.encodeResponse(request.header(), request.header().apiVersion(), response);
    }
}
```

FILE 8/15: `lakewire-broker/src/main/java/lakewire/broker/network/RequestHandler.java`

```java
package lakewire.broker.network;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import lakewire.broker.api.RequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One instance per connection. Each request runs on its own virtual thread, but requests on one
 * connection are processed one after another and answered in arrival order. Different connections
 * run in parallel. The Netty event loop only copies bytes and writes responses; it never blocks.
 */
public class RequestHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private static final Logger log = LoggerFactory.getLogger(RequestHandler.class);

    /** Above this many unanswered requests we stop reading from the socket until some are answered. */
    static final int MAX_IN_FLIGHT_PER_CONNECTION = 16;

    private final RequestProcessor processor;
    private final ExecutorService requestExecutor;

    // Only touched on this channel's event-loop thread, so no locking is needed.
    private CompletableFuture<Void> previousWrite = CompletableFuture.completedFuture(null);
    private int inFlight;

    public RequestHandler(RequestProcessor processor, ExecutorService requestExecutor) {
        this.processor = processor;
        this.requestExecutor = requestExecutor;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf frame) {
        // Netty frees `frame` when this method returns, so copy the bytes for the virtual thread.
        ByteBuffer request = ByteBuffer.wrap(ByteBufUtil.getBytes(frame));

        // Start processing only after the previous response on this connection has been written,
        // so requests on one connection are handled one at a time, in order (like Kafka's broker).
        CompletableFuture<ByteBuffer> response =
            previousWrite.thenApplyAsync(ignored -> processor.process(request), requestExecutor);

        // Write (or close) back on the event loop; the next request waits for this step.
        previousWrite = response.handleAsync((buffer, failure) -> writeOrClose(ctx, buffer, failure), ctx.executor());

        inFlight++;
        if (inFlight >= MAX_IN_FLIGHT_PER_CONNECTION) {
            ctx.channel().config().setAutoRead(false);
        }
    }

    private Void writeOrClose(ChannelHandlerContext ctx, ByteBuffer buffer, Throwable failure) {
        inFlight--;
        if (failure != null) {
            if (ctx.channel().isActive()) {
                log.warn("Closing connection {}: {}", ctx.channel().remoteAddress(), rootCause(failure).toString());
                ctx.close();
            }
            // Re-throw so the chain stays failed and later requests on this connection are never processed.
            throw failure instanceof CompletionException completion ? completion : new CompletionException(failure);
        }
        if (!ctx.channel().isActive()) {
            return null;
        }
        ctx.writeAndFlush(Unpooled.wrappedBuffer(buffer));
        if (!ctx.channel().config().isAutoRead() && inFlight < MAX_IN_FLIGHT_PER_CONNECTION) {
            ctx.channel().config().setAutoRead(true);
        }
        return null;
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable cause = failure;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("Closing connection {}: {}", ctx.channel().remoteAddress(), cause.toString());
        ctx.close();
    }
}
```

FILE 9/15: `lakewire-broker/src/main/java/lakewire/broker/network/KafkaServer.java`

```java
package lakewire.broker.network;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lakewire.broker.api.RequestProcessor;
import lakewire.broker.api.RequestRouter;
import lakewire.broker.config.BrokerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaServer {

    private static final Logger log = LoggerFactory.getLogger(KafkaServer.class);

    private static final long REQUEST_DRAIN_SECONDS = 10;

    private final BrokerConfig config;
    private final RequestProcessor processor;
    private ExecutorService requestExecutor;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private volatile boolean running;

    public KafkaServer(BrokerConfig config) {
        this(config, new RequestRouter());
    }

    KafkaServer(BrokerConfig config, RequestProcessor processor) {
        this.config = config;
        this.processor = processor;
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        requestExecutor = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("lakewire-request-", 0).factory());
        bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        workerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                            .addLast("frameEncoder", new LengthFieldPrepender(4))
                            .addLast("frameDecoder",
                                new LengthFieldBasedFrameDecoder(config.maxFrameBytes(), 0, 4, 0, 4))
                            .addLast("requestHandler", new RequestHandler(processor, requestExecutor));
                    }
                });
            serverChannel = bootstrap.bind(config.host(), config.port()).sync().channel();
            running = true;
            log.info("LakeWire broker listening on {}", serverChannel.localAddress());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            stop();
            throw new IllegalStateException("Interrupted while binding", e);
        } catch (RuntimeException e) {
            stop();
            throw e;
        }
    }

    public synchronized void stop() {
        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
            serverChannel = null;
        }
        if (requestExecutor != null) {
            drain(requestExecutor);
            requestExecutor = null;
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
            bossGroup = null;
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
            workerGroup = null;
        }
        running = false;
    }

    /** Lets in-flight requests finish (their responses still get written), then interrupts stragglers. */
    private static void drain(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(REQUEST_DRAIN_SECONDS, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public boolean isRunning() {
        return running;
    }

    public int boundPort() {
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }
}
```

FILE 10/15: `lakewire-broker/src/test/java/lakewire/broker/network/KafkaTestClient.java`

```java
package lakewire.broker.network;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

/** A tiny blocking client that speaks Kafka framing: a 4-byte size, then the bytes. */
final class KafkaTestClient implements AutoCloseable {

    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;

    KafkaTestClient(int port) throws IOException {
        socket = new Socket("127.0.0.1", port);
        socket.setSoTimeout(5_000);
        in = new DataInputStream(socket.getInputStream());
        out = new DataOutputStream(socket.getOutputStream());
    }

    void send(ByteBuffer frame) throws IOException {
        byte[] bytes = new byte[frame.remaining()];
        frame.get(bytes);
        out.writeInt(bytes.length);
        out.write(bytes);
        out.flush();
    }

    ByteBuffer receive() throws IOException {
        byte[] bytes = new byte[in.readInt()];
        in.readFully(bytes);
        return ByteBuffer.wrap(bytes);
    }

    /** Returns -1 once the server has closed the connection. */
    int readByteOrEof() throws IOException {
        return in.read();
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
```

FILE 11/15: `lakewire-protocol/src/test/java/lakewire/protocol/SupportedApiTest.java`

```java
package lakewire.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionCollection;
import org.junit.jupiter.api.Test;

class SupportedApiTest {

    @Test
    void everyAdvertisedRangeIsParseableByKafkaClients() {
        for (SupportedApi api : SupportedApi.values()) {
            assertThat(api.minVersion()).isGreaterThanOrEqualTo(api.apiKey().oldestVersion());
            assertThat(api.maxVersion()).isLessThanOrEqualTo(api.apiKey().latestVersion());
            assertThat(api.minVersion()).isLessThanOrEqualTo(api.maxVersion());
        }
    }

    @Test
    void advertisesExactlyTheRegistry() {
        ApiVersionCollection advertised = SupportedApi.advertisedApiVersions();
        assertThat(advertised).hasSize(SupportedApi.values().length);
        ApiVersion apiVersions = advertised.find((short) 18);
        assertThat(apiVersions.minVersion()).isEqualTo((short) 0);
        assertThat(apiVersions.maxVersion()).isEqualTo((short) 3);
    }
}
```

FILE 12/15: `lakewire-protocol/src/test/java/lakewire/protocol/KafkaCodecTest.java`

```java
package lakewire.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;

class KafkaCodecTest {

    @Test
    void decodesFlexibleV3RequestBuiltByKafka() {
        ApiVersionsRequest original = new ApiVersionsRequest.Builder((short) 3).build((short) 3);
        RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, (short) 3, "codec-test", 7);
        ByteBuffer frame = original.serializeWithHeader(header);

        KafkaRequest decoded = KafkaCodec.decodeRequest(frame);

        assertThat(decoded.header().apiKey()).isEqualTo(ApiKeys.API_VERSIONS);
        assertThat(decoded.header().apiVersion()).isEqualTo((short) 3);
        assertThat(decoded.header().correlationId()).isEqualTo(7);
        assertThat(decoded.header().clientId()).isEqualTo("codec-test");
        ApiVersionsRequest body = (ApiVersionsRequest) decoded.body();
        assertThat(body.data().clientSoftwareName()).isEqualTo("apache-kafka-java");
        assertThat(body.hasUnsupportedRequestVersion()).isFalse();
    }

    @Test
    void encodedResponseIsReadableByKafka() {
        RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, (short) 3, "codec-test", 11);
        ApiVersionsResponse response = new ApiVersionsResponse(new ApiVersionsResponseData()
            .setErrorCode(Errors.NONE.code())
            .setApiKeys(SupportedApi.advertisedApiVersions()));

        ByteBuffer bytes = KafkaCodec.encodeResponse(header, (short) 3, response);

        ApiVersionsResponse parsed = (ApiVersionsResponse) AbstractResponse.parseResponse(bytes, header);
        assertThat(parsed.data().errorCode()).isEqualTo(Errors.NONE.code());
        assertThat(parsed.data().apiKeys().find((short) 18).maxVersion()).isEqualTo((short) 3);
    }

    @Test
    void unknownApiKeyIsRejected() {
        ByteBuffer frame = ByteBuffer.allocate(10)
            .putShort((short) 9999).putShort((short) 0).putInt(1).putShort((short) -1)
            .flip();

        assertThatThrownBy(() -> KafkaCodec.decodeRequest(frame))
            .isInstanceOf(InvalidRequestException.class);
    }
}
```

FILE 13/15: `lakewire-broker/src/test/java/lakewire/broker/network/ApiVersionsTest.java`

```java
package lakewire.broker.network;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import lakewire.broker.config.BrokerConfig;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.DescribeClusterRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.DescribeClusterRequest;
import org.apache.kafka.common.requests.DescribeClusterResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ApiVersionsTest {

    private KafkaServer server;

    @BeforeEach
    void startServer() {
        server = new KafkaServer(new BrokerConfig("127.0.0.1", 0, 1_048_576));
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop();
    }

    @Test
    void apiVersionsV0ReturnsSupportedApis() throws Exception {
        try (Socket socket = new Socket("127.0.0.1", server.boundPort())) {
            socket.setSoTimeout(5_000);
            sendRawRequest(socket, (short) 18, (short) 0, 42, false);
            DataInputStream in = new DataInputStream(socket.getInputStream());

            int size = in.readInt();
            assertThat(in.readInt()).isEqualTo(42);
            assertThat(in.readShort()).isEqualTo(Errors.NONE.code());
            assertThat(in.readInt()).isEqualTo(1);
            assertThat(in.readShort()).isEqualTo((short) 18);
            assertThat(in.readShort()).isEqualTo((short) 0);
            assertThat(in.readShort()).isEqualTo((short) 3);
            assertThat(size).isEqualTo(16);
        }
    }

    @Test
    void unsupportedVersionReturnsError35WithV0Body() throws Exception {
        try (Socket socket = new Socket("127.0.0.1", server.boundPort())) {
            socket.setSoTimeout(5_000);
            // v99 is "newer than anything we know"; such a client sends the flexible v2 header,
            // which ends with an (empty) tagged-field section.
            sendRawRequest(socket, (short) 18, (short) 99, 7, true);
            DataInputStream in = new DataInputStream(socket.getInputStream());

            int size = in.readInt();
            assertThat(in.readInt()).isEqualTo(7);
            assertThat(in.readShort()).isEqualTo(Errors.UNSUPPORTED_VERSION.code());
            assertThat(in.readInt()).isEqualTo(1);
            assertThat(in.readShort()).isEqualTo((short) 18);
            assertThat(in.readShort()).isEqualTo((short) 0);
            assertThat(in.readShort()).isEqualTo((short) 3);
            assertThat(size).isEqualTo(16);
        }
    }

    @Test
    void flexibleV3RequestBuiltByKafkaGetsV3Response() throws Exception {
        RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, (short) 3, "lakewire-test", 99);
        ApiVersionsRequest request = new ApiVersionsRequest.Builder((short) 3).build((short) 3);

        try (KafkaTestClient client = new KafkaTestClient(server.boundPort())) {
            client.send(request.serializeWithHeader(header));
            ApiVersionsResponse response =
                (ApiVersionsResponse) AbstractResponse.parseResponse(client.receive(), header);

            assertThat(response.data().errorCode()).isEqualTo(Errors.NONE.code());
            assertThat(response.data().apiKeys()).hasSize(1);
            ApiVersion apiVersions = response.apiVersion(ApiKeys.API_VERSIONS.id);
            assertThat(apiVersions.minVersion()).isEqualTo((short) 0);
            assertThat(apiVersions.maxVersion()).isEqualTo((short) 3);
        }
    }

    @Test
    void versionKafkaKnowsButWeDontAdvertiseFallsBackToV0() throws Exception {
        short newest = ApiKeys.API_VERSIONS.latestVersion();
        RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, newest, "lakewire-test", 5);
        ApiVersionsRequest request = new ApiVersionsRequest.Builder(newest).build(newest);

        try (KafkaTestClient client = new KafkaTestClient(server.boundPort())) {
            client.send(request.serializeWithHeader(header));
            ByteBuffer bytes = client.receive();

            ResponseHeader responseHeader = ResponseHeader.parse(bytes, (short) 0);
            ApiVersionsResponse response = ApiVersionsResponse.parse(new ByteBufferAccessor(bytes), (short) 0);
            assertThat(responseHeader.correlationId()).isEqualTo(5);
            assertThat(response.data().errorCode()).isEqualTo(Errors.UNSUPPORTED_VERSION.code());
            assertThat(response.apiVersion(ApiKeys.API_VERSIONS.id).maxVersion()).isEqualTo((short) 3);
        }
    }

    @Test
    void invalidClientSoftwareNameGetsInvalidRequest() throws Exception {
        ApiVersionsRequestData data = new ApiVersionsRequestData()
            .setClientSoftwareName("not a valid name!")
            .setClientSoftwareVersion("1.0");
        RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, (short) 3, "lakewire-test", 8);
        ApiVersionsRequest request =
            new ApiVersionsRequest.Builder(data, (short) 3, (short) 3).build((short) 3);

        try (KafkaTestClient client = new KafkaTestClient(server.boundPort())) {
            client.send(request.serializeWithHeader(header));
            ApiVersionsResponse response =
                (ApiVersionsResponse) AbstractResponse.parseResponse(client.receive(), header);
            assertThat(response.data().errorCode()).isEqualTo(Errors.INVALID_REQUEST.code());
        }
    }

    @Test
    void kafkaApiWeDontImplementGetsUnsupportedVersion() throws Exception {
        short version = ApiKeys.DESCRIBE_CLUSTER.latestVersion();
        RequestHeader header = new RequestHeader(ApiKeys.DESCRIBE_CLUSTER, version, "lakewire-test", 12);
        DescribeClusterRequest request =
            new DescribeClusterRequest.Builder(new DescribeClusterRequestData()).build(version);

        try (KafkaTestClient client = new KafkaTestClient(server.boundPort())) {
            client.send(request.serializeWithHeader(header));
            DescribeClusterResponse response =
                (DescribeClusterResponse) AbstractResponse.parseResponse(client.receive(), header);
            assertThat(response.data().errorCode()).isEqualTo(Errors.UNSUPPORTED_VERSION.code());
        }
    }

    @Test
    void unknownApiKeyClosesTheConnection() throws Exception {
        try (Socket socket = new Socket("127.0.0.1", server.boundPort())) {
            socket.setSoTimeout(5_000);
            sendRawRequest(socket, (short) 9999, (short) 0, 1, false);
            assertThat(socket.getInputStream().read()).isEqualTo(-1);
        }
    }

    private static void sendRawRequest(Socket socket, short apiKey, short version, int correlationId,
                                       boolean flexibleHeader) throws Exception {
        byte[] clientId = "test".getBytes(StandardCharsets.UTF_8);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        out.writeInt(2 + 2 + 4 + 2 + clientId.length + (flexibleHeader ? 1 : 0));
        out.writeShort(apiKey);
        out.writeShort(version);
        out.writeInt(correlationId);
        out.writeShort(clientId.length);
        out.write(clientId);
        if (flexibleHeader) {
            out.writeByte(0); // zero tagged fields
        }
        out.flush();
    }
}
```

FILE 14/15: `lakewire-broker/src/test/java/lakewire/broker/network/KafkaServerTest.java`

```java
package lakewire.broker.network;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetSocketAddress;
import java.net.Socket;
import lakewire.broker.config.BrokerConfig;
import org.junit.jupiter.api.Test;

class KafkaServerTest {

    @Test
    void startsAcceptsConnectionsAndStops() throws Exception {
        KafkaServer server = new KafkaServer(new BrokerConfig("127.0.0.1", 0, 1_048_576));
        server.start();
        try {
            assertThat(server.isRunning()).isTrue();
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", server.boundPort()), 2_000);
                assertThat(socket.isConnected()).isTrue();
            }
        } finally {
            server.stop();
        }
        assertThat(server.isRunning()).isFalse();
    }
}
```

FILE 15/15: `lakewire-broker/src/test/java/lakewire/broker/network/RequestOrderingTest.java`

```java
package lakewire.broker.network;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lakewire.broker.config.BrokerConfig;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;

class RequestOrderingTest {

    private static final BrokerConfig CONFIG = new BrokerConfig("127.0.0.1", 0, 1_048_576);

    @Test
    void requestsOnOneConnectionAreProcessedOneAtATimeInOrder() throws Exception {
        int requests = 10;
        AtomicInteger running = new AtomicInteger();
        AtomicInteger maxRunning = new AtomicInteger();
        List<Integer> processOrder = new CopyOnWriteArrayList<>();
        List<Boolean> ranOnVirtualThread = new CopyOnWriteArrayList<>();

        // Echoes the 4-byte id it receives. Early requests sleep longer, so if the broker ran them
        // at the same time, later ones would overlap with them and finish first.
        KafkaServer server = new KafkaServer(CONFIG, frame -> {
            int id = frame.getInt();
            maxRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
            ranOnVirtualThread.add(Thread.currentThread().isVirtual());
            sleep((requests - id) * 20L);
            processOrder.add(id);
            running.decrementAndGet();
            return ByteBuffer.allocate(4).putInt(0, id);
        });
        server.start();
        try (KafkaTestClient client = new KafkaTestClient(server.boundPort())) {
            for (int id = 0; id < requests; id++) {
                client.send(ByteBuffer.allocate(4).putInt(0, id));
            }
            List<Integer> received = new ArrayList<>();
            for (int i = 0; i < requests; i++) {
                received.add(client.receive().getInt());
            }

            assertThat(received).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
            assertThat(processOrder).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
            assertThat(maxRunning.get()).isEqualTo(1);
            assertThat(ranOnVirtualThread).containsOnly(true);
        } finally {
            server.stop();
        }
    }

    @Test
    void differentConnectionsAreProcessedInParallel() throws Exception {
        // Each handler waits until the other connection's handler has also started.
        // If connections were processed one after another, the latch would time out (5 s).
        CountDownLatch bothStarted = new CountDownLatch(2);
        List<Boolean> sawOtherConnection = new CopyOnWriteArrayList<>();
        List<Boolean> ranOnVirtualThread = new CopyOnWriteArrayList<>();

        KafkaServer server = new KafkaServer(CONFIG, frame -> {
            int id = frame.getInt();
            ranOnVirtualThread.add(Thread.currentThread().isVirtual());
            bothStarted.countDown();
            try {
                sawOtherConnection.add(bothStarted.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            sleep(500);
            return ByteBuffer.allocate(4).putInt(0, id);
        });
        server.start();
        try (KafkaTestClient first = new KafkaTestClient(server.boundPort());
             KafkaTestClient second = new KafkaTestClient(server.boundPort())) {
            long startNanos = System.nanoTime();
            first.send(ByteBuffer.allocate(4).putInt(0, 1));
            second.send(ByteBuffer.allocate(4).putInt(0, 2));

            assertThat(first.receive().getInt()).isEqualTo(1);
            assertThat(second.receive().getInt()).isEqualTo(2);
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

            assertThat(sawOtherConnection).containsExactly(true, true);
            assertThat(ranOnVirtualThread).containsOnly(true);
            // Two 500 ms requests in parallel take ~500 ms; one after another would take 5 s or more.
            assertThat(elapsedMillis).isLessThan(3_000);
        } finally {
            server.stop();
        }
    }

    @Test
    void earlierResponsesAreSentBeforeAFailedRequestClosesTheConnection() throws Exception {
        List<Integer> processed = new CopyOnWriteArrayList<>();
        KafkaServer server = new KafkaServer(CONFIG, frame -> {
            int id = frame.getInt();
            processed.add(id);
            if (id == 2) {
                throw new IllegalArgumentException("bad request " + id);
            }
            sleep(id == 0 ? 200 : 0);
            return ByteBuffer.allocate(4).putInt(0, id);
        });
        server.start();
        try (KafkaTestClient client = new KafkaTestClient(server.boundPort())) {
            for (int id = 0; id < 4; id++) {
                client.send(ByteBuffer.allocate(4).putInt(0, id));
            }
            assertThat(client.receive().getInt()).isEqualTo(0);
            assertThat(client.receive().getInt()).isEqualTo(1);
            assertThat(client.readByteOrEof()).isEqualTo(-1);
            // Request 3 arrived after the bad one, so it must never be processed.
            assertThat(processed).containsExactly(0, 1, 2);
        } finally {
            server.stop();
        }
    }

    @Test
    void manyPipelinedApiVersionsRequestsAreAnsweredInOrder() throws Exception {
        int requests = 200; // well above MAX_IN_FLIGHT_PER_CONNECTION, so reading pauses and resumes
        KafkaServer server = new KafkaServer(CONFIG);
        server.start();
        try (KafkaTestClient client = new KafkaTestClient(server.boundPort())) {
            List<RequestHeader> headers = new ArrayList<>();
            for (int correlationId = 1000; correlationId < 1000 + requests; correlationId++) {
                RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, (short) 3, "pipeline", correlationId);
                headers.add(header);
                client.send(new ApiVersionsRequest.Builder((short) 3).build((short) 3).serializeWithHeader(header));
            }
            for (RequestHeader header : headers) {
                // parseResponse throws CorrelationIdMismatchException if a response is out of order.
                ApiVersionsResponse response =
                    (ApiVersionsResponse) AbstractResponse.parseResponse(client.receive(), header);
                assertThat(response.data().errorCode()).isEqualTo(Errors.NONE.code());
            }
        } finally {
            server.stop();
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
```

STEP 3: Verify. From the project root, in a terminal, run:
`mvnw.cmd clean verify`
Expected: BUILD SUCCESS, with `LakeWire Protocol` reporting `Tests run: 5, Failures: 0, Errors: 0, Skipped: 0` and `LakeWire Broker` reporting `Tests run: 12, Failures: 0, Errors: 0, Skipped: 0` (17 tests in total). Two WARN log lines from RequestHandler ("bad request 2" and "Unexpected api key: 9999") are expected; tests trigger them on purpose.

STEP 4: Report and stop. Reply with:
1. the list of files deleted and the list of files written,
2. the two `Tests run:` summary lines and the BUILD result, copied from the output,
3. if anything failed, the first 40 lines starting at the first `[ERROR]`.
Do not run the broker, do not commit, and do not make any further changes.
