package lakewire.broker.api;

import java.nio.ByteBuffer;
import java.util.Optional;

/** Turns one request frame into one response frame. Runs on a virtual thread, so it may block. */
@FunctionalInterface
public interface RequestProcessor {

    /**
     * @param request the frame without its length prefix
     * @return the response (header + body) without its length prefix, or empty when the client
     *     must not be answered (acks=0)
     * @throws RuntimeException if the request is unreadable; the connection is then closed
     */
    Optional<ByteBuffer> process(ByteBuffer request);
}
