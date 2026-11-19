package lakewire.verify;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import lakewire.verify.nemesis.NemesisKind;

/** Seeded nemesis plan. The same seed and inputs produce the same events. */
public final class Scheduler {

    public record Event(long atMs, NemesisKind kind, int broker, long holdMs) {
    }

    private Scheduler() {
    }

    public static List<Event> plan(long seed, Duration duration, List<NemesisKind> kinds, int brokers) {
        SplittableRandom random = new SplittableRandom(seed);
        List<Event> events = new ArrayList<>();
        long cursor = 2_000 + random.nextLong(3_000);
        long end = duration.toMillis();
        while (cursor < end - 1_000) {
            NemesisKind kind = kinds.get(random.nextInt(kinds.size()));
            int broker = brokers <= 1 ? 0 : random.nextInt(brokers);
            long hold = 400 + random.nextLong(1_600);
            if (kind == NemesisKind.PAUSE) {
                hold = 4_000 + random.nextLong(3_000);
            }
            events.add(new Event(cursor, kind, broker, hold));
            cursor += 4_000 + random.nextLong(8_000);
        }
        return List.copyOf(events);
    }
}
