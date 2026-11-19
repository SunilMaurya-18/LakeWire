package lakewire.verify;

import lakewire.verify.checker.AckLog;

public record HistoryEvent(
    String kind,
    String key,
    int partition,
    long offset,
    int producer,
    int seq,
    int hash
) {

    public AckLog toAckLog() {
        return new AckLog(key, hash, partition, offset);
    }

    public String toJson() {
        return "{\"kind\":\"" + kind
            + "\",\"key\":\"" + escape(key)
            + "\",\"partition\":" + partition
            + ",\"offset\":" + offset
            + ",\"producer\":" + producer
            + ",\"seq\":" + seq
            + ",\"hash\":" + hash
            + "}";
    }

    public static HistoryEvent parse(String line) {
        return new HistoryEvent(
            field(line, "kind", true),
            field(line, "key", true),
            Integer.parseInt(field(line, "partition", false)),
            Long.parseLong(field(line, "offset", false)),
            Integer.parseInt(field(line, "producer", false)),
            Integer.parseInt(field(line, "seq", false)),
            Integer.parseInt(field(line, "hash", false))
        );
    }

    private static String field(String json, String name, boolean quoted) {
        String needle = "\"" + name + "\":";
        int at = json.indexOf(needle);
        if (at < 0) {
            throw new IllegalArgumentException("missing " + name);
        }
        int start = at + needle.length();
        if (quoted) {
            if (json.charAt(start) != '"') {
                throw new IllegalArgumentException("expected string " + name);
            }
            int end = json.indexOf('"', start + 1);
            return json.substring(start + 1, end);
        }
        int end = start;
        while (end < json.length()) {
            char ch = json.charAt(end);
            if (ch == ',' || ch == '}') {
                break;
            }
            end++;
        }
        return json.substring(start, end);
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
