package lakewire.broker.write;

import java.util.List;
import lakewire.coordinator.CommitResult;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class WritePipelineFenceTest {

    @Test
    void onlyAFullNotLeaderResultIsAFence() {
        short notLeader = Errors.NOT_LEADER_OR_FOLLOWER.code();
        assertThat(WritePipeline.fenced(List.of())).isFalse();
        assertThat(WritePipeline.fenced(List.of(new CommitResult(0, -1, notLeader, 0)))).isTrue();
        assertThat(WritePipeline.fenced(List.of(
            new CommitResult(0, -1, notLeader, 0),
            new CommitResult(1, -1, notLeader, 0)
        ))).isTrue();
        assertThat(WritePipeline.fenced(List.of(
            new CommitResult(0, -1, notLeader, 0),
            new CommitResult(1, 3, Errors.NONE.code(), 0)
        ))).isFalse();
        assertThat(WritePipeline.fenced(List.of(
            new CommitResult(0, -1, Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), 0)
        ))).isFalse();
    }
}
