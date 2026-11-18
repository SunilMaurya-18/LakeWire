package lakewire.storage.lkw1;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import lakewire.storage.RecordChunks;
import org.apache.kafka.common.compress.Compression;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Lkw1RoundTripTest {

    private static final long SEED = Long.getLong("lakewire.test.seed", 42L);

    @Test
    @Timeout(180)
    void tenThousandObjectsRoundTripByteForByte() {
        System.out.println("Lkw1RoundTripTest seed=" + SEED);
        SplittableRandom random = new SplittableRandom(SEED);
        for (int object = 0; object < 10_000; object++) {
            List<Lkw1Chunk> chunks = chunks(random);
            Lkw1Object written = new Lkw1Object("broker-" + (object % 7), object, 1_700_000_000_000L + object, chunks);
            byte[] bytes = Lkw1Writer.write(written);
            Lkw1Object read = Lkw1Reader.read(bytes);
            assertThat(read.brokerId()).as("seed %s object %s broker", SEED, object).isEqualTo(written.brokerId());
            assertThat(read.brokerEpoch()).as("seed %s object %s epoch", SEED, object).isEqualTo(written.brokerEpoch());
            assertThat(read.createdMs()).as("seed %s object %s created", SEED, object).isEqualTo(written.createdMs());
            assertThat(read.chunks()).as("seed %s object %s chunks", SEED, object).hasSize(chunks.size());
            for (int i = 0; i < chunks.size(); i++) {
                assertThat(read.chunks().get(i).records())
                    .as("seed %s object %s chunk %s", SEED, object, i)
                    .isEqualTo(chunks.get(i).records());
                assertThat(read.chunks().get(i).nRecords()).isEqualTo(chunks.get(i).nRecords());
                assertThat(read.chunks().get(i).nBatches()).isEqualTo(chunks.get(i).nBatches());
                assertThat(read.chunks().get(i).maxTimestamp()).isEqualTo(chunks.get(i).maxTimestamp());
                assertThat(read.chunks().get(i).topicId()).isEqualTo(chunks.get(i).topicId());
                assertThat(read.chunks().get(i).part()).isEqualTo(chunks.get(i).part());
            }
        }
    }

    @Test
    void oneFlippedByteIsRejected() {
        byte[] records = RecordChunks.oneBatch(Compression.NONE, 2, 10);
        byte[] bytes = Lkw1Writer.write(new Lkw1Object("b", 1, 2, List.of(
            new Lkw1Chunk(1, 0, records, 2, (short) 1, 11)
        )));
        int body = bytes.length / 2;
        bytes[body] ^= 0x01;
        assertThatThrownBy(() -> Lkw1Reader.read(bytes))
            .isInstanceOf(Lkw1Exception.class)
            .hasMessageContaining("crc");
    }

    private static List<Lkw1Chunk> chunks(SplittableRandom random) {
        int partitions = 1 + random.nextInt(5);
        List<Lkw1Chunk> chunks = new ArrayList<>(partitions);
        for (int part = 0; part < partitions; part++) {
            int batches = 1 + random.nextInt(3);
            Compression compression = random.nextBoolean() ? Compression.NONE : Compression.gzip().build();
            ByteBuffer joined = ByteBuffer.allocate(8 * 1024);
            int records = 0;
            long maxTimestamp = 0;
            for (int batch = 0; batch < batches; batch++) {
                int count = 1 + random.nextInt(4);
                int valueSize = 1 + random.nextInt(32);
                long timestamp = random.nextInt(1_000_000);
                byte[] written = batch(compression, count, valueSize, timestamp, random);
                joined.put(written);
                records += count;
                maxTimestamp = Math.max(maxTimestamp, timestamp + count - 1);
            }
            byte[] recordsBytes = new byte[joined.position()];
            joined.flip();
            joined.get(recordsBytes);
            chunks.add(new Lkw1Chunk(
                random.nextLong(1, 20),
                part,
                recordsBytes,
                records,
                (short) batches,
                maxTimestamp
            ));
        }
        return chunks;
    }

    private static byte[] batch(
        Compression compression,
        int count,
        int valueSize,
        long timestamp,
        SplittableRandom random
    ) {
        ByteBuffer buffer = ByteBuffer.allocate(512 + count * (valueSize + 64));
        var builder = org.apache.kafka.common.record.internal.MemoryRecords.builder(
            buffer,
            compression,
            org.apache.kafka.common.record.TimestampType.CREATE_TIME,
            0L
        );
        for (int i = 0; i < count; i++) {
            byte[] value = new byte[valueSize];
            random.nextBytes(value);
            builder.append(timestamp + i, new byte[] {(byte) i}, value);
        }
        var written = builder.build();
        ByteBuffer bytes = written.buffer();
        byte[] copy = new byte[bytes.remaining()];
        bytes.get(copy);
        return copy;
    }
}
