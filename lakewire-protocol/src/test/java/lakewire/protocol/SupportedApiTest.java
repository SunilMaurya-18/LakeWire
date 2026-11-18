package lakewire.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionCollection;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;

class SupportedApiTest {

    @Test
    void everyAdvertisedRangeIsParseableByKafkaClients() {
        for (SupportedApi api : SupportedApi.values()) {
            assertThat(api.minVersion()).isGreaterThanOrEqualTo(api.apiKey().oldestVersion());
            assertThat(api.maxVersion()).isLessThanOrEqualTo(api.apiKey().latestVersion());
            assertThat(api.minVersion()).isLessThanOrEqualTo(api.maxVersion());
        }
    }

    @Test
    void advertisesExactlyTheRegistry() {
        ApiVersionCollection advertised = SupportedApi.advertisedApiVersions();
        assertThat(advertised).hasSize(SupportedApi.values().length);
        ApiVersion apiVersions = advertised.find((short) 18);
        assertThat(apiVersions.minVersion()).isEqualTo((short) 0);
        assertThat(apiVersions.maxVersion()).isEqualTo((short) 3);
    }

    @Test
    void pinnedRangesStayBelowTopicIdVersions() {
        assertRange(ApiKeys.API_VERSIONS, 0, 3);
        assertRange(ApiKeys.METADATA, 1, 9);
        assertRange(ApiKeys.CREATE_TOPICS, 2, 6);
        assertRange(ApiKeys.DELETE_TOPICS, 1, 5);
    }

    private static void assertRange(ApiKeys apiKey, int min, int max) {
        SupportedApi api = SupportedApi.forApiKey(apiKey).orElseThrow();
        assertThat(api.minVersion()).isEqualTo((short) min);
        assertThat(api.maxVersion()).isEqualTo((short) max);
    }
}
