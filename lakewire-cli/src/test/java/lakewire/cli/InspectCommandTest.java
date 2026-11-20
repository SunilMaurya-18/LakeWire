package lakewire.cli;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import lakewire.storage.lkw1.Lkw1Chunk;
import lakewire.storage.lkw1.Lkw1Object;
import lakewire.storage.lkw1.Lkw1Writer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class InspectCommandTest {

    @Test
    void printsHeaderAndIndexFromALocalFile(@TempDir Path dir) throws Exception {
        byte[] body = Lkw1Writer.write(new Lkw1Object(
            "1",
            7,
            1_700_000_000_000L,
            List.of(new Lkw1Chunk(4, 0, new byte[] {1, 2, 3, 4}, 1, (short) 1, 99))
        ));
        Path file = dir.resolve("sample.lkw");
        Files.write(file, body);
        StringWriter out = new StringWriter();
        int code = LakewireCli.execute(
            new String[] {"inspect", "object", file.toString()},
            new PrintWriter(out, true),
            new PrintWriter(out, true)
        );
        assertThat(code).isZero();
        assertThat(out.toString())
            .contains("LKW1 brokerId=1 epoch=7 createdMs=1700000000000")
            .contains("topic=4 part=0 nRecords=1 nBatches=1 maxTs=99 bytes=4");
    }
}
