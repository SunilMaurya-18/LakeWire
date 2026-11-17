package lakewire.broker.api;

import java.nio.ByteBuffer;

@FunctionalInterface
public interface RequestProcessor {
    ByteBuffer process(ByteBuffer request);

}
