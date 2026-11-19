package lakewire.cost;

import java.math.BigDecimal;

public record CostEstimate(
    BigDecimal classicProducerCrossAz,
    BigDecimal classicReplication,
    BigDecimal classicTransfer,
    BigDecimal lakewireProducerCrossAz,
    BigDecimal lakewirePuts,
    BigDecimal lakewireGets,
    BigDecimal lakewireTransfer,
    BigDecimal tieredTransfer,
    BigDecimal classicStorageGbMonth,
    BigDecimal lakewireStorageGbMonth,
    BigDecimal tieredStorageGbMonth
) {
}
