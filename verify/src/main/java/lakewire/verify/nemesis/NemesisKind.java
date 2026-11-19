package lakewire.verify.nemesis;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public enum NemesisKind {
    KILL("kill"),
    PAUSE("pause"),
    PARTITION_PG("partition-pg"),
    SLOW_S3("slow-s3"),
    PG_RESTART("pg-restart"),
    RETENTION_STORM("retention-storm");

    private final String flag;

    NemesisKind(String flag) {
        this.flag = flag;
    }

    public String flag() {
        return flag;
    }

    public static NemesisKind parse(String raw) {
        String name = raw.trim().toLowerCase(Locale.ROOT);
        for (NemesisKind kind : values()) {
            if (kind.flag.equals(name)) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown nemesis " + raw);
    }

    public static List<NemesisKind> parseList(String raw) {
        List<NemesisKind> kinds = new ArrayList<>();
        for (String part : raw.split(",")) {
            if (!part.isBlank()) {
                kinds.add(parse(part));
            }
        }
        if (kinds.isEmpty()) {
            throw new IllegalArgumentException("nemesis list is empty");
        }
        return List.copyOf(kinds);
    }
}
