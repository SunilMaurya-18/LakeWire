package lakewire.cost;

import java.math.BigDecimal;
import java.nio.file.Path;

/** Prints the §12.1 model. {@code lakewire cost} in M11 wraps this. */
public final class CostMain {

    private CostMain() {
    }

    public static void main(String[] args) {
        System.out.print(run(args));
    }

    public static String run(String[] args) {
        BigDecimal gib = BigDecimal.ONE;
        int zones = 3;
        int objectMib = 8;
        int retentionDays = 7;
        int hotDays = 1;
        Path prices = null;
        for (int i = 0; i < args.length; i++) {
            String flag = args[i];
            switch (flag) {
                case "--prices" -> prices = Path.of(require(args, ++i, flag));
                case "--gib", "--gib-per-day" -> gib = new BigDecimal(require(args, ++i, flag));
                case "--zones" -> zones = Integer.parseInt(require(args, ++i, flag));
                case "--object-mib" -> objectMib = Integer.parseInt(require(args, ++i, flag));
                case "--retention-days" -> retentionDays = Integer.parseInt(require(args, ++i, flag));
                case "--hot-days" -> hotDays = Integer.parseInt(require(args, ++i, flag));
                default -> throw new IllegalArgumentException("unknown argument " + flag);
            }
        }
        CostPrices loaded = prices == null ? CostPrices.defaults() : CostPrices.fromFile(prices);
        CostQuery query = new CostQuery(
            gib, zones, objectMib * 1024 * 1024, retentionDays, hotDays);
        return new CostModel(loaded).render(query);
    }

    private static String require(String[] args, int index, String flag) {
        if (index >= args.length) {
            throw new IllegalArgumentException(flag + " needs a value");
        }
        return args[index];
    }
}
