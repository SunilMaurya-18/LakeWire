package lakewire.storage;

/** Size in bytes and the unsigned CRC32C of the object body. CRC is -1 when the store has none. */
public record ObjectHead(long size, long crc32c) {
}
