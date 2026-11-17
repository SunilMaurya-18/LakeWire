package lakewire.broker.network;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.Optional;
import lakewire.protocol.ApiKey;
import lakewire.protocol.RequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class RequestHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private static final Logger log = LoggerFactory.getLogger(RequestHandler.class);

    private final ApiVersionsHandler apiVersionsHandler;

    RequestHandler(ApiVersionsHandler apiVersionsHandler) {
        this.apiVersionsHandler = apiVersionsHandler;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf frame) {
        RequestHeader header = RequestHeader.parse(frame);
        Optional<ApiKey> api = ApiKey.fromId(header.ApiKey());

        if (api.isEmpty()) {
            log.warn("Unsupported api_key {} from {} (client_id={}), closing connection",
                    header.ApiKey(), ctx.channel().remoteAddress(), header.clientId());
            ctx.close();
            return;
        }

        switch (api.get()) {
            case API_VERSIONS -> ctx.writeAndFlush(apiVersionsHandler.handle(header, ctx.alloc()));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("Closing connection {}: {}", ctx.channel().remoteAddress(), cause.toString());
        ctx.close();
    }
}
