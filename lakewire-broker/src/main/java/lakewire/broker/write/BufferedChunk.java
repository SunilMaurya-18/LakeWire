package lakewire.broker.write;

import java.util.concurrent.CompletableFuture;

public final class BufferedChunk {

    private final Append append;
    private final CompletableFuture<ProduceAck> ack = new CompletableFuture<>();

    BufferedChunk(Append append) {
        this.append = append;
    }

    public Append append() {
        return append;
    }

    public CompletableFuture<ProduceAck> ack() {
        return ack;
    }

    public int bytes() {
        return append.records().length;
    }
}
