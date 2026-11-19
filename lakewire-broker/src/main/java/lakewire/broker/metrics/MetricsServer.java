package lakewire.broker.metrics;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** JDK {@code HttpServer} on {@code LAKEWIRE_METRICS_PORT} (A18). */
public final class MetricsServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MetricsServer.class);

    private final HttpServer server;

    public MetricsServer(String host, int port, LakeWireMetrics metrics) {
        try {
            server = HttpServer.create(new InetSocketAddress(host, port), 0);
        } catch (IOException failure) {
            throw new UncheckedIOException("metrics server bind failed on " + host + ":" + port, failure);
        }
        server.createContext("/metrics", exchange -> {
            byte[] body = metrics.scrape().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        log.info("metrics on {}:{}", boundHost(), boundPort());
    }

    public int boundPort() {
        return server.getAddress().getPort();
    }

    public String boundHost() {
        return server.getAddress().getHostString();
    }

    @Override
    public void close() {
        server.stop(0);
        if (server.getExecutor() instanceof ExecutorService pool) {
            pool.shutdownNow();
        }
    }
}
