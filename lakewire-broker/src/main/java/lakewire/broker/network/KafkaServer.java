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

    public KafkaServer(BrokerConfig config, RequestProcessor processor) {
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
