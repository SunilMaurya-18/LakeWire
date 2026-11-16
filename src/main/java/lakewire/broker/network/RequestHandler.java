package lakewire.broker.network;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class RequestHandler extends SimpleChannelInboundHandler<ByteBuf> {
    private static final Logger log = LoggerFactory.getLogger(RequestHandler.class);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf frame) {
        log.debug("Received {}-byte request from {}", frame.readableBytes(), ctx.channel().remoteAddress());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("Caught exception {}: {}", ctx.channel().remoteAddress(), cause.toString());
        ctx.close();

    }
}
