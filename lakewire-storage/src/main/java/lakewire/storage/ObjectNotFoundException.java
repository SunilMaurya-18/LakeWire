package lakewire.storage;

public final class ObjectNotFoundException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ObjectNotFoundException(String key) {
        super("object not found: " + key);
    }

    public ObjectNotFoundException(String key, Throwable cause) {
        super("object not found: " + key, cause);
    }
}
