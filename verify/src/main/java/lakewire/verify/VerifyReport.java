package lakewire.verify;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import lakewire.verify.nemesis.NemesisKind;

public record VerifyReport(
    String checkerVersion,
    long seed,
    String duration,
    List<String> nemeses,
    long acks,
    long reads,
    long events,
    List<String> violations
) {

    public static final String CHECKER_VERSION = "v0.1-m9";

    public boolean ok() {
        return violations.isEmpty();
    }

    public void write(Path dir) throws IOException {
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("verify-report.json"), toJson());
        Files.writeString(dir.resolve("verify-report.md"), toMarkdown());
    }

    public String toJson() {
        String violationJson = violations.stream()
            .map(v -> "\"" + v.replace("\\", "\\\\").replace("\"", "\\\"") + "\"")
            .collect(Collectors.joining(","));
        String nemesisJson = nemeses.stream()
            .map(v -> "\"" + v + "\"")
            .collect(Collectors.joining(","));
        return "{\n"
            + "  \"checkerVersion\": \"" + checkerVersion + "\",\n"
            + "  \"seed\": " + seed + ",\n"
            + "  \"duration\": \"" + duration + "\",\n"
            + "  \"nemeses\": [" + nemesisJson + "],\n"
            + "  \"ops\": {\"acks\": " + acks + ", \"reads\": " + reads + ", \"events\": " + events + "},\n"
            + "  \"violations\": [" + violationJson + "]\n"
            + "}\n";
    }

    public String toMarkdown() {
        StringBuilder md = new StringBuilder();
        md.append("# verify report\n\n");
        md.append("- checker: ").append(checkerVersion).append('\n');
        md.append("- seed: ").append(seed).append('\n');
        md.append("- duration: ").append(duration).append('\n');
        md.append("- nemeses: ").append(String.join(", ", nemeses)).append('\n');
        md.append("- acks: ").append(acks).append(" reads: ").append(reads).append(" events: ").append(events).append('\n');
        md.append("- result: ").append(ok() ? "PASS" : "FAIL").append('\n');
        if (!violations.isEmpty()) {
            md.append("\n## violations\n");
            for (String violation : violations) {
                md.append("- ").append(violation).append('\n');
            }
        }
        return md.toString();
    }

    public static List<String> flags(List<NemesisKind> kinds) {
        return kinds.stream().map(NemesisKind::flag).toList();
    }
}
