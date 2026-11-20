package lakewire.cli;

import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "dev", description = "Embedded Postgres, local FS, one broker on :9092")
final class DevCommand implements Callable<Integer> {

    @Option(names = "--home", description = "Data directory (default: ~/.lakewire/dev)")
    Path home;

    @Option(names = "--port", defaultValue = "9092")
    int port;

    @Option(names = "--metrics-port", defaultValue = "9094")
    int metricsPort;

    @Override
    public Integer call() throws InterruptedException {
        Path dir = home == null ? DevServer.defaultHome() : home;
        DevServer server = DevServer.start(dir, port, metricsPort);
        CountDownLatch done = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(Thread.ofPlatform().unstarted(() -> {
            server.close();
            done.countDown();
        }));
        System.out.println("ready bootstrap=localhost:" + server.port() + "  (" + server.readyMs() + " ms)");
        done.await();
        return 0;
    }
}
