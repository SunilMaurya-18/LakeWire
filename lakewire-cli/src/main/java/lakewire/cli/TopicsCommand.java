package lakewire.cli;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

@Command(
    name = "topics",
    mixinStandardHelpOptions = true,
    description = "Create, list, describe and delete topics",
    subcommands = {
        TopicsCommand.Create.class,
        TopicsCommand.ListTopics.class,
        TopicsCommand.Describe.class,
        TopicsCommand.Delete.class
    }
)
final class TopicsCommand {

    @Option(names = "--bootstrap", defaultValue = "localhost:9092")
    String bootstrap;

    static Admin admin(String bootstrap) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "20000");
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "lakewire-cli");
        return Admin.create(props);
    }

    @Command(name = "create", description = "Create a topic")
    static final class Create implements Callable<Integer> {

        @ParentCommand
        TopicsCommand parent;

        @Parameters(index = "0")
        String name;

        @Option(names = "-p", defaultValue = "1")
        int partitions;

        @Option(names = "--retention", defaultValue = "7d")
        String retention;

        @Override
        public Integer call() throws Exception {
            long retentionMs = Durations.toMillis(retention);
            NewTopic topic = new NewTopic(name, partitions, (short) 1)
                .configs(Map.of("retention.ms", Long.toString(retentionMs)));
            try (Admin admin = admin(parent.bootstrap)) {
                admin.createTopics(List.of(topic)).all().get(20, TimeUnit.SECONDS);
            }
            System.out.println("created " + name + " partitions=" + partitions);
            return 0;
        }
    }

    @Command(name = "list", description = "List topics")
    static final class ListTopics implements Callable<Integer> {

        @ParentCommand
        TopicsCommand parent;

        @Override
        public Integer call() throws Exception {
            try (Admin admin = admin(parent.bootstrap)) {
                for (String name : admin.listTopics().names().get(20, TimeUnit.SECONDS).stream().sorted().toList()) {
                    System.out.println(name);
                }
            }
            return 0;
        }
    }

    @Command(name = "describe", description = "Describe a topic")
    static final class Describe implements Callable<Integer> {

        @ParentCommand
        TopicsCommand parent;

        @Parameters(index = "0")
        String name;

        @Override
        public Integer call() throws Exception {
            try (Admin admin = admin(parent.bootstrap)) {
                TopicDescription description = admin.describeTopics(List.of(name))
                    .allTopicNames()
                    .get(20, TimeUnit.SECONDS)
                    .get(name);
                System.out.println(name + " partitions=" + description.partitions().size());
                description.partitions().forEach(partition ->
                    System.out.println("  " + partition.partition() + " leader=" + partition.leader().id()));
            }
            return 0;
        }
    }

    @Command(name = "delete", description = "Delete a topic")
    static final class Delete implements Callable<Integer> {

        @ParentCommand
        TopicsCommand parent;

        @Parameters(index = "0")
        String name;

        @Override
        public Integer call() throws Exception {
            try (Admin admin = admin(parent.bootstrap)) {
                admin.deleteTopics(List.of(name)).all().get(20, TimeUnit.SECONDS);
            }
            System.out.println("deleted " + name);
            return 0;
        }
    }
}
