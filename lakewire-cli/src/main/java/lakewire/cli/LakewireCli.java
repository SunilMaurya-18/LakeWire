package lakewire.cli;

import java.io.PrintWriter;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(
    name = "lakewire",
    mixinStandardHelpOptions = true,
    version = "0.1.0-SNAPSHOT",
    description = "LakeWire CLI",
    subcommands = {
        DevCommand.class,
        BrokerCommand.class,
        WorkerCommand.class,
        TopicsCommand.class,
        CostCommand.class,
        VerifyCommand.class,
        InspectCommand.class,
        CommandLine.HelpCommand.class
    }
)
public final class LakewireCli implements Callable<Integer> {

    @Override
    public Integer call() {
        CommandLine.usage(this, System.out);
        return 0;
    }

    public static void main(String[] args) {
        System.exit(execute(args, null, null));
    }

    public static int execute(String[] args, PrintWriter out, PrintWriter err) {
        CommandLine command = new CommandLine(new LakewireCli());
        if (out != null) {
            command.setOut(out);
        }
        if (err != null) {
            command.setErr(err);
        }
        command.setExecutionExceptionHandler((ex, cmd, parse) -> {
            cmd.getErr().println(ex.getMessage() == null ? ex.toString() : ex.getMessage());
            return 1;
        });
        return command.execute(args);
    }
}
