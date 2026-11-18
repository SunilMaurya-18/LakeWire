package lakewire.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class ObjectStoreChecks {

    private ObjectStoreChecks() {
    }

    static void verifyAll(ObjectStore store) {
        byte[] body = {10, 20, 30, 40, 50, 60};
        StorageAwait.await(store.put("logs/a", body));
        assertThatThrownBy(() -> StorageAwait.await(store.put("logs/a", body)))
            .isInstanceOf(ObjectAlreadyExistsException.class);
        assertThat(StorageAwait.await(store.getRange("logs/a", 2, 3))).containsExactly(30, 40, 50);
        assertThat(StorageAwait.await(store.head("logs/a")))
            .isEqualTo(new ObjectHead(body.length, Crc32c.of(body)));

        StorageAwait.await(store.put("logs/b", new byte[] {1}));
        StorageAwait.await(store.put("other", new byte[] {2}));
        assertThat(StorageAwait.await(store.list("logs/"))).containsExactly("logs/a", "logs/b");

        assertThatThrownBy(() -> StorageAwait.await(store.getRange("missing", 0, 1)))
            .isInstanceOf(ObjectNotFoundException.class);
        assertThatThrownBy(() -> StorageAwait.await(store.head("missing")))
            .isInstanceOf(ObjectNotFoundException.class);
        assertThatThrownBy(() -> StorageAwait.await(store.delete("missing")))
            .isInstanceOf(ObjectNotFoundException.class);

        StorageAwait.await(store.delete("logs/a"));
        assertThatThrownBy(() -> StorageAwait.await(store.getRange("logs/a", 0, 1)))
            .isInstanceOf(ObjectNotFoundException.class);
        assertThat(StorageAwait.await(store.list("logs/"))).containsExactly("logs/b");
    }
}
