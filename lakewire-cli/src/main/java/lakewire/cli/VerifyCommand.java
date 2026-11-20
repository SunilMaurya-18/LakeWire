package lakewire.cli;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import lakewire.verify.LakeWireVerify;
import lakewire.verify.VerifyArgs;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "verify", description = "Run the fault-injection harness")
final class VerifyCommand implements Callable<Integer> {

    @Option(names = "--nemesis", defaultValue = "kill,pause,partition-pg")
    String nemesis;

    @Option(names = "--duration", defaultValue = "10m")
    String duration;

    @Option(names = "--seed", defaultValue = "42")
    long seed;

    @Option(names = "--output")
    Path output;

    @Option(names = "--soak", defaultValue = "1")
    int soak;

    @Override
    public Integer call() throws Exception {
        List<String> args = new ArrayList<>();
        args.add("--nemesis");
        args.add(nemesis);
        args.add("--duration");
        args.add(duration);
        args.add("--seed");
        args.add(Long.toString(seed));
        args.add("--soak");
        args.add(Integer.toString(soak));
        if (output != null) {
            args.add("--output");
            args.add(output.toString());
        }
        return LakeWireVerify.run(VerifyArgs.parse(args.toArray(String[]::new)));
    }
}
