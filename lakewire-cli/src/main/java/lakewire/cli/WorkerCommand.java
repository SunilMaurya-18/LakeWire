package lakewire.cli;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import lakewire.broker.BrokerRuntime;
import lakewire.broker.config.BrokerConfig;
import lakewire.coordinator.Coordinator;
import lakewire.coordinator.CoordinatorConfig;
import lakewire.storage.ObjectStore;
import lakewire.workers.WorkerConfig;
import lakewire.workers.WorkerMetrics;
import lakewire.workers.WorkerRuntime;
import picocli.CommandLine.Command;

@Command(name = "worker", description = "Run object-lifecycle workers without a Kafka socket")
final class WorkerCommand implements Callable<Integer> {

    @Override
    public Integer call() throws InterruptedException {
        Map<String, String> env = System.getenv();
        CoordinatorConfig coordinatorConfig = CoordinatorConfig.fromEnvironment(env);
        BrokerConfig brokerConfig = BrokerConfig.fromEnvironment(env);
        ObjectStore store = BrokerRuntime.openStore(env, brokerConfig);
        Coordinator coordinator = new Coordinator(coordinatorConfig);
        coordinator.start();
        WorkerRuntime workers = new WorkerRuntime(
            WorkerConfig.fromEnvironment(env), coordinator, store, new WorkerMetrics());
        workers.start();
        CountDownLatch done = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(Thread.ofPlatform().unstarted(() -> {
            workers.close();
            coordinator.close();
            store.close();
            done.countDown();
        }));
        done.await();
        return 0;
    }
}
