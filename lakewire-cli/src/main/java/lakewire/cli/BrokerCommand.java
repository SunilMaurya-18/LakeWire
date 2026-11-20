package lakewire.cli;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import lakewire.broker.LakeWireBroker;
import picocli.CommandLine.Command;

@Command(name = "broker", description = "Run a LakeWire broker")
final class BrokerCommand implements Callable<Integer> {

    @Override
    public Integer call() throws InterruptedException {
        LakeWireBroker.main(new String[0]);
        CountDownLatch done = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(Thread.ofPlatform().unstarted(done::countDown));
        done.await();
        return 0;
    }
}
