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
