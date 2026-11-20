package lakewire.cli;

import java.util.Locale;

final class Durations {

    private Durations() {
    }

    static long toMillis(String raw) {
        String value = raw.trim().toLowerCase(Locale.ROOT);
        if (value.endsWith("ms")) {
            return Long.parseLong(value.substring(0, value.length() - 2));
        }
        if (value.endsWith("d")) {
            return Long.parseLong(value.substring(0, value.length() - 1)) * 86_400_000L;
        }
        if (value.endsWith("h")) {
            return Long.parseLong(value.substring(0, value.length() - 1)) * 3_600_000L;
        }
        if (value.endsWith("m")) {
            return Long.parseLong(value.substring(0, value.length() - 1)) * 60_000L;
        }
        if (value.endsWith("s")) {
            return Long.parseLong(value.substring(0, value.length() - 1)) * 1_000L;
        }
        return Long.parseLong(value);
    }

    static int toDays(String raw) {
        long millis = toMillis(raw);
        long days = millis / 86_400_000L;
        if (days > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("duration too large: " + raw);
        }
        return (int) days;
    }
}
