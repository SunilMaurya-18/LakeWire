package lakewire.protocol;

import java.util.Optional;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionCollection;
import org.apache.kafka.common.protocol.ApiKeys;

/**
 * The single registry of Kafka APIs that LakeWire implements, with the version range we answer.
 * ApiVersions advertises exactly this list (PRD §10.1), so add an entry here only when its handler exists.
 */
public enum SupportedApi {
    API_VERSIONS(ApiKeys.API_VERSIONS, (short) 0, (short) 3),
    METADATA(ApiKeys.METADATA, (short) 1, (short) 9),
        PRODUCE(ApiKeys.PRODUCE, (short) 3, (short) 12),
        FETCH(ApiKeys.FETCH, (short) 4, (short) 12),
        LIST_OFFSETS(ApiKeys.LIST_OFFSETS, (short) 1, (short) 6),
        CREATE_TOPICS(ApiKeys.CREATE_TOPICS, (short) 2, (short) 6),
    DELETE_TOPICS(ApiKeys.DELETE_TOPICS, (short) 1, (short) 5);

    private final ApiKeys apiKey;
    private final short minVersion;
    private final short maxVersion;

    SupportedApi(ApiKeys apiKey, short minVersion, short maxVersion) {
        this.apiKey = apiKey;
        this.minVersion = minVersion;
        this.maxVersion = maxVersion;
    }

    public ApiKeys apiKey() {
        return apiKey;
    }

    public short minVersion() {
        return minVersion;
    }

    public short maxVersion() {
        return maxVersion;
    }

    public boolean supportsVersion(short version) {
        return version >= minVersion && version <= maxVersion;
    }

    public static Optional<SupportedApi> forApiKey(ApiKeys apiKey) {
        for (SupportedApi api : values()) {
            if (api.apiKey == apiKey) {
                return Optional.of(api);
            }
        }
        return Optional.empty();
    }

    /** Builds a fresh collection each call: Kafka's collection links its elements, so they can't be shared. */
    public static ApiVersionCollection advertisedApiVersions() {
        ApiVersionCollection collection = new ApiVersionCollection();
        for (SupportedApi api : values()) {
            collection.add(new ApiVersion()
                .setApiKey(api.apiKey.id)
                .setMinVersion(api.minVersion)
                .setMaxVersion(api.maxVersion));
        }
        return collection;
    }
}
