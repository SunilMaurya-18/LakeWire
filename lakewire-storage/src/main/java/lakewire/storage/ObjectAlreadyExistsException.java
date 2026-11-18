package lakewire.storage;

public final class ObjectAlreadyExistsException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ObjectAlreadyExistsException(String key) {
        super("object already exists: " + key);
    }

    public ObjectAlreadyExistsException(String key, Throwable cause) {
        super("object already exists: " + key, cause);
    }
}
