package lakewire.storage;

public final class TransientStorageException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public TransientStorageException(String message) {
        super(message);
    }

    public TransientStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
