package lakewire.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionCollection;
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
}
