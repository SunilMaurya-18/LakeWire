package lakewire.broker.write;

import java.util.concurrent.CompletableFuture;

/** What {@code ProduceHandler} needs from the write path. */
public interface ProduceSink {

    boolean accepting();

    int objectMaxBytes();

    CompletableFuture<ProduceAck> append(Append append, long waitMs);
}
