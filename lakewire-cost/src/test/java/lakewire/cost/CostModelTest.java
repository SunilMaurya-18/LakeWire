package lakewire.cost;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CostModelTest {

    @Test
    void section12_1TableValuesToFourDecimalPlaces() {
        CostEstimate estimate = CostModel.defaults().estimate(CostQuery.tableDefault());
        assertThat(estimate.classicProducerCrossAz()).isEqualByComparingTo("0.0133");
        assertThat(estimate.classicReplication()).isEqualByComparingTo("0.0400");
        assertThat(estimate.classicTransfer()).isEqualByComparingTo("0.0533");
        assertThat(estimate.lakewireProducerCrossAz()).isEqualByComparingTo("0.0000");
        assertThat(estimate.lakewirePuts()).isEqualByComparingTo("0.0006");
        assertThat(estimate.lakewireGets()).isEqualByComparingTo("0.0002");
        assertThat(estimate.lakewireTransfer()).isEqualByComparingTo("0.0008");
        assertThat(estimate.tieredTransfer()).isEqualByComparingTo(estimate.classicTransfer());
        assertThat(estimate.lakewireStorageGbMonth()).isEqualByComparingTo("0.0230");
        assertThat(estimate.classicStorageGbMonth()).isEqualByComparingTo("0.2400");
    }

    @Test
    void renderAlwaysPrintsCaveats() {
        String text = CostModel.defaults().render(CostQuery.tableDefault());
        assertThat(text).contains("Honest caveats");
        assertThat(text).contains("Postgres");
        assertThat(text).contains("$0.0533");
        assertThat(text).contains("$0.0008");
    }

    @Test
    void pricesFileOverride() {
        CostPrices prices = new CostPrices(
            new BigDecimal("0.02"),
            new BigDecimal("0.005"),
            new BigDecimal("0.0004"),
            new BigDecimal("0.023"),
            new BigDecimal("0.08")
        );
        CostEstimate estimate = new CostModel(prices).estimate(CostQuery.tableDefault());
        assertThat(estimate.classicTransfer()).isEqualByComparingTo("0.0533");
    }
}
