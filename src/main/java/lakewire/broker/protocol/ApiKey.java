package lakewire.broker.protocol;

import java.util.Arrays;
import java.util.Optional;

public enum ApiKey {
    API_VERSION((short) 18, (short) 0, (short) 3);
    private final short id;
    private final short minVersion;
    private final short maxVersion;

    ApiKey(short id, short minVersion, short maxVersion) {
        this.id = id;
        this.minVersion = minVersion;
        this.maxVersion = maxVersion;
    }

    public short id() {
        return id;
    }

    public short minVersion() {
        return minVersion;
    }

    public short maxVersion() {
        return maxVersion;
    }

    public boolean support(short version) {
        return version >= this.minVersion && version <= this.maxVersion;
    }

    public static Optional<ApiKey> fromId(short id) {
        return Arrays.stream(values()).filter(k -> k.id == id).findFirst();

    }
}
