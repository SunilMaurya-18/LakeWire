package lakewire.broker.network;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import lakewire.broker.config.BrokerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class KafkaServer {

    private static final Logger log = LoggerFactory.getLogger(KafkaServer.class);
    private final ApiVersionsHandler apiVersionsHandler = new ApiVersionsHandler();
    private final BrokerConfig props;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private volatile boolean running;

    public KafkaServer(BrokerConfig props) {
        this.props = props;
    }

    public synchronized void start() {
        bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        workerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast("framePrepender", new LengthFieldPrepender(4))
                                    .addLast("frameDecoder",
                                            new LengthFieldBasedFrameDecoder(props.maxFrameBytes(), 0, 4, 0, 4))
                                    .addLast("requestHandler", new RequestHandler(apiVersionsHandler));
                        }
                    });
            serverChannel = bootstrap.bind(props.host(), props.port())
                    .syncUninterruptibly()
                    .channel();
            running = true;
            log.info("LakeWire broker listening on {}", serverChannel.localAddress());
        } catch (Exception e) {
            stop();
            throw new IllegalStateException("Could not find " + props.host() + ":" + props.port(), e);
        }
    }

    public synchronized void stop() {
        running = false;
        if (serverChannel != null) {
            serverChannel.close().awaitUninterruptibly();
            serverChannel = null;
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully().syncUninterruptibly();
            bossGroup = null;
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully().syncUninterruptibly();
            workerGroup = null;
        }
    }

    public boolean isRunning() {
        return running;
    }

    public int boundPort() {
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

}
