package lakewire.cost;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

/**
 * Per-GiB written and per-GB-month storage from PRD §12.1.
 * One GiB is 2<sup>30</sup> bytes so 8 MiB objects are 128 objects/GiB.
 */
public final class CostModel {

    public static final String CAVEATS = """
        Honest caveats. Consumer cross-AZ traffic depends on fetch-from-follower setup. \
        Postgres (e.g. a managed instance) is a fixed monthly cost that dominates at low volume. \
        Small objects (low traffic + short linger) raise PUT costs, and the per-shard buffers \
        in a sharded coordinator multiply PUTs. Latency is higher than disk-based Kafka. \
        Classic storage is 3 × EBS list price with no extra headroom multiplier (A51). \
        Kafka tiered storage uses the same transfer as classic; the hot set is 1 day on disk (A9).""";

    private static final BigDecimal GIB_BYTES = BigDecimal.valueOf(1L << 30);
    private static final BigDecimal THOUSAND = BigDecimal.valueOf(1000);
    private static final MathContext MATH = new MathContext(16, RoundingMode.HALF_UP);
    private static final int TABLE_SCALE = 4;

    private final CostPrices prices;

    public CostModel(CostPrices prices) {
        this.prices = prices;
    }

    public static CostModel defaults() {
        return new CostModel(CostPrices.defaults());
    }

    public CostEstimate estimate(CostQuery query) {
        BigDecimal objects = GIB_BYTES.divide(BigDecimal.valueOf(query.objectBytes()), MATH)
            .multiply(query.gibWritten());
        BigDecimal zones = BigDecimal.valueOf(query.zones());
        BigDecimal remoteShare = BigDecimal.valueOf(query.zones() - 1)
            .divide(zones, MATH);

        BigDecimal classicProducer = query.gibWritten()
            .multiply(remoteShare)
            .multiply(prices.interAzPerGb());
        BigDecimal classicReplication = query.gibWritten()
            .multiply(BigDecimal.valueOf(2))
            .multiply(prices.interAzPerGb());
        BigDecimal classicTransfer = classicProducer.add(classicReplication);

        BigDecimal puts = objects.multiply(prices.s3PutPerThousand()).divide(THOUSAND, MATH);
        BigDecimal gets = objects.multiply(zones).multiply(prices.s3GetPerThousand()).divide(THOUSAND, MATH);
        BigDecimal lakewireTransfer = puts.add(gets);

        BigDecimal classicStorage = prices.ebsGbMonth().multiply(BigDecimal.valueOf(3));
        BigDecimal lakewireStorage = prices.s3GbMonth();
        BigDecimal hotFraction = hotFraction(query);
        BigDecimal tieredStorage = classicStorage.multiply(hotFraction)
            .add(lakewireStorage.multiply(BigDecimal.ONE.subtract(hotFraction)));

        return new CostEstimate(
            round(classicProducer),
            round(classicReplication),
            round(classicTransfer),
            round(BigDecimal.ZERO),
            round(puts),
            round(gets),
            round(lakewireTransfer),
            round(classicTransfer),
            round(classicStorage),
            round(lakewireStorage),
            round(tieredStorage)
        );
    }

    public String render(CostQuery query) {
        CostEstimate estimate = estimate(query);
        return """
            Per GiB written (%s zones, %s MiB objects)  [laptop model]
            Classic Kafka RF=3     producer→leader  %s
                                   replication      %s
                                   transfer total   %s
            LakeWire               PUTs             %s
                                   GETs             %s
                                   transfer total   %s
            Kafka tiered storage   transfer total   %s  (same as classic)

            Storage (per GB-month)
            Classic Kafka          3 × EBS          %s
            LakeWire               S3 Standard      %s
            Kafka tiered storage   1d hot + S3      %s

            %s
            """.formatted(
            query.zones(),
            query.objectBytes() / (1024 * 1024),
            money(estimate.classicProducerCrossAz()),
            money(estimate.classicReplication()),
            money(estimate.classicTransfer()),
            money(estimate.lakewirePuts()),
            money(estimate.lakewireGets()),
            money(estimate.lakewireTransfer()),
            money(estimate.tieredTransfer()),
            money(estimate.classicStorageGbMonth()),
            money(estimate.lakewireStorageGbMonth()),
            money(estimate.tieredStorageGbMonth()),
            CAVEATS
        );
    }

    private static BigDecimal hotFraction(CostQuery query) {
        if (query.retentionDays() <= 0) {
            return BigDecimal.ZERO;
        }
        BigDecimal hot = BigDecimal.valueOf(query.hotDays())
            .divide(BigDecimal.valueOf(query.retentionDays()), MATH);
        if (hot.compareTo(BigDecimal.ONE) > 0) {
            return BigDecimal.ONE;
        }
        return hot;
    }

    private static BigDecimal round(BigDecimal value) {
        return value.setScale(TABLE_SCALE, RoundingMode.HALF_UP);
    }

    private static String money(BigDecimal value) {
        return "$" + value.toPlainString();
    }
}
