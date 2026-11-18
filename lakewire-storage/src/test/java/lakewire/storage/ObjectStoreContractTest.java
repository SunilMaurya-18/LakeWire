package lakewire.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

abstract class ObjectStoreContractTest {

    private ObjectStore store;

    protected abstract ObjectStore open();

    @BeforeEach
    void openStore() {
        store = open();
    }

    @AfterEach
    void closeStore() {
        if (store != null) {
            store.close();
        }
    }

    @Test
    void createOnlyRangeHeadListAndDelete() {
        ObjectStoreChecks.verifyAll(store);
    }
}
