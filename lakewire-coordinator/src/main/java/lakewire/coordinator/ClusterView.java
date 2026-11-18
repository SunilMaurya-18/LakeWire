package lakewire.coordinator;

import java.util.List;

/** What the broker needs from Postgres for Metadata, CreateTopics and DeleteTopics. */
public interface ClusterView {

    List<LiveBroker> liveBrokers();

    String clusterId();

    List<StoredTopic> topics();

    TopicMutation createTopic(String name, int partitions, long retentionMs, boolean validateOnly);

    short deleteTopic(String name);
}
