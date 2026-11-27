package lakewire.cli;

import java.io.BufferedWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LakewireCliTest {

    @Test
    void helpListsSubcommands() {
        StringWriter out = new StringWriter();
        int code = LakewireCli.execute(new String[] {"--help"}, new PrintWriter(out, true), new PrintWriter(out, true));
        assertThat(code).isZero();
        assertThat(out.toString()).contains("dev", "broker", "worker", "topics", "cost", "verify", "inspect");
    }

    @Test
    void topicsHelpMentionsCreate() {
        StringWriter out = new StringWriter();
        int code = LakewireCli.execute(
            new String[] {"topics", "--help"}, new PrintWriter(out, true), new PrintWriter(out, true));
        assertThat(code).isZero();
        assertThat(out.toString()).contains("create", "list", "describe", "delete");
    }

    @Test
    void costReproducesSection12Table() {
        StringWriter out = new StringWriter();
        int code = LakewireCli.execute(
            new String[] {"cost", "--gib-per-day", "1", "--zones", "3", "--retention", "7d"},
            new PrintWriter(out, true),
            new PrintWriter(out, true)
        );
        assertThat(code).isZero();
        assertThat(out.toString()).contains("$0.0533").contains("$0.0008");
    }

    @Test
    void costFlushesBufferedOutput() {
        StringWriter out = new StringWriter();
        int code = LakewireCli.execute(
            new String[] {"cost"}, new PrintWriter(new BufferedWriter(out)), new PrintWriter(out, true));
        assertThat(code).isZero();
        assertThat(out.toString()).contains("$0.0533");
    }
}
