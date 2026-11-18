package lakewire.storage;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalFsObjectStoreTest extends ObjectStoreContractTest {

    @TempDir
    Path root;

    @Override
    protected ObjectStore open() {
        return new LocalFsObjectStore(root);
    }

    @Test
    void rejectsAKeyThatEscapesTheRoot() {
        ObjectStore store = open();
        try {
            assertThatThrownBy(() -> StorageAwait.await(store.put("../outside", new byte[] {1})))
                .isInstanceOf(IllegalArgumentException.class);
        } finally {
            store.close();
        }
    }
}
