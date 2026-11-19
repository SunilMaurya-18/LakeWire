package lakewire.verify;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import lakewire.verify.nemesis.NemesisKind;

public record VerifyArgs(
    List<NemesisKind> nemeses,
    Duration duration,
    long seed,
    Path output,
    int soak
) {

    public static VerifyArgs parse(String[] args) {
        List<NemesisKind> nemeses = List.of(NemesisKind.KILL, NemesisKind.PAUSE, NemesisKind.PARTITION_PG);
        Duration duration = Duration.ofMinutes(10);
        long seed = 42;
        Path output = Path.of("target", "verify-run");
        int soak = 1;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "--nemesis" -> nemeses = NemesisKind.parseList(require(args, ++i, arg));
                case "--duration" -> duration = parseDuration(require(args, ++i, arg));
                case "--seed" -> seed = Long.parseLong(require(args, ++i, arg));
                case "--output" -> output = Path.of(require(args, ++i, arg));
                case "--soak" -> soak = Integer.parseInt(require(args, ++i, arg));
                default -> throw new IllegalArgumentException("unknown argument " + arg);
            }
        }
        if (soak < 1) {
            throw new IllegalArgumentException("--soak must be >= 1");
        }
        return new VerifyArgs(List.copyOf(nemeses), duration, seed, output, soak);
    }

    static Duration parseDuration(String raw) {
        String value = raw.trim().toLowerCase(Locale.ROOT);
        if (value.endsWith("ms")) {
            return Duration.ofMillis(Long.parseLong(value.substring(0, value.length() - 2)));
        }
        if (value.endsWith("s")) {
            return Duration.ofSeconds(Long.parseLong(value.substring(0, value.length() - 1)));
        }
        if (value.endsWith("m")) {
            return Duration.ofMinutes(Long.parseLong(value.substring(0, value.length() - 1)));
        }
        return Duration.ofMillis(Long.parseLong(value));
    }

    private static String require(String[] args, int index, String flag) {
        if (index >= args.length) {
            throw new IllegalArgumentException(flag + " needs a value");
        }
        return args[index];
    }
}
