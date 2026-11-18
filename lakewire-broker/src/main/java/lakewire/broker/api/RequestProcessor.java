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
