package lakewire.verify.checker;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CheckerTest {

    @Test
    void i1PassesMatchingHistoriesAndFailsLostWrite() {
        AckLog ack = new AckLog("k", 1, 0, 0);
        assertThatCode(() -> I1AckedReadableChecker.assertReadable(List.of(ack), List.of(ack)))
            .doesNotThrowAnyException();
        assertThatThrownBy(() -> I1AckedReadableChecker.assertAllAckedPresent(List.of(ack), List.of()))
            .isInstanceOf(AssertionError.class)
            .hasMessageContaining("acked");
    }

    @Test
    void i2PassesContiguousSpansAndFailsAGap() {
        assertThatCode(() -> I2ContiguityChecker.assertSpans(0, 5, 10, List.of(new long[] {5, 9})))
            .doesNotThrowAnyException();
        assertThatThrownBy(() -> I2ContiguityChecker.assertSpans(0, 0, 4, List.of(new long[] {0, 1}, new long[] {3, 3})))
            .isInstanceOf(AssertionError.class)
            .hasMessageContaining("breaks");
    }

    @Test
    void i3PassesMonotonicSequencesAndFailsAGap() {
        List<StoredSequences.Sequence> ok = List.of(
            new StoredSequences.Sequence(0, 1, 0),
            new StoredSequences.Sequence(0, 1, 1)
        );
        assertThatCode(() -> I3SequenceOrderChecker.assertMonotonic(ok)).doesNotThrowAnyException();
        assertThatThrownBy(() -> I3SequenceOrderChecker.assertMonotonic(List.of(
            new StoredSequences.Sequence(0, 1, 0),
            new StoredSequences.Sequence(0, 1, 2)
        ))).isInstanceOf(AssertionError.class);
    }

    @Test
    void i4PassesStableReadsAndFailsPhantomAndHw() {
        AckLog rec = new AckLog("k", 7, 0, 3);
        assertThatCode(() -> I4PhantomChecker.assertStable(List.of(rec), List.of(rec), Map.of(0, 4L)))
            .doesNotThrowAnyException();
        assertThatThrownBy(() -> I4PhantomChecker.assertStable(
            List.of(rec), List.of(), Map.of(0, 4L)))
            .isInstanceOf(AssertionError.class)
            .hasMessageContaining("phantom");
        assertThatThrownBy(() -> I4PhantomChecker.assertStable(
            List.of(rec), List.of(rec), Map.of(0, 3L)))
            .isInstanceOf(AssertionError.class)
            .hasMessageContaining("HW");
    }

    @Test
    void i5PassesUniqueSequencesAndFailsDuplicate() {
        StoredSequences.Sequence one = new StoredSequences.Sequence(0, 9, 1);
        assertThatCode(() -> I5NoDuplicateChecker.assertUnique(List.of(one))).doesNotThrowAnyException();
        assertThatThrownBy(() -> I5NoDuplicateChecker.assertUnique(List.of(one, one)))
            .isInstanceOf(AssertionError.class)
            .hasMessageContaining("duplicate");
    }

    @Test
    void i7PassesExistingKeysAndFailsDanglingRef() {
        assertThatCode(() -> I7NoDanglingRefChecker.assertModel(Set.of("a"), Set.of("a", "b")))
            .doesNotThrowAnyException();
        assertThatThrownBy(() -> I7NoDanglingRefChecker.assertModel(Set.of("missing"), Set.of("a")))
            .isInstanceOf(AssertionError.class)
            .hasMessageContaining("dangling");
    }

    @Test
    void i8PassesYoungLeaksAndFailsOldLeaks() {
        Instant now = Instant.parse("2026-11-19T12:00:00Z");
        Duration max = Duration.ofMinutes(11);
        assertThatCode(() -> I8NoLeakChecker.assertModel(now.minusSeconds(60), now, max, "young"))
            .doesNotThrowAnyException();
        assertThatThrownBy(() -> I8NoLeakChecker.assertModel(now.minus(Duration.ofHours(1)), now, max, "old"))
            .isInstanceOf(AssertionError.class)
            .hasMessageContaining("I8 leak");
    }
}
