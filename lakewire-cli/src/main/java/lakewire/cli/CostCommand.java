package lakewire.cli;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import lakewire.cost.CostModel;
import lakewire.cost.CostPrices;
import lakewire.cost.CostQuery;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Model.CommandSpec;

@Command(name = "cost", description = "Print the §12.1 Kafka vs LakeWire vs tiered cost model")
final class CostCommand implements Callable<Integer> {

    @Spec
    CommandSpec spec;

    @Option(names = "--gib-per-day", defaultValue = "1")
    BigDecimal gibPerDay;

    @Option(names = "--zones", defaultValue = "3")
    int zones;

    @Option(names = "--retention", defaultValue = "7d")
    String retention;

    @Option(names = "--object-mib", defaultValue = "8")
    int objectMib;

    @Option(names = "--prices")
    Path prices;

    @Override
    public Integer call() {
        CostPrices loaded = prices == null ? CostPrices.defaults() : CostPrices.fromFile(prices);
        CostQuery query = new CostQuery(
            gibPerDay,
            zones,
            objectMib * 1024 * 1024,
            Durations.toDays(retention),
            1
        );
        spec.commandLine().getOut().print(new CostModel(loaded).render(query));
        return 0;
    }
}
