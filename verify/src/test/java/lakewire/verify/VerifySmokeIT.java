package lakewire.verify;

import java.nio.file.Path;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.DockerClientFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

@Order(4)
class VerifySmokeIT {

    @Test
    @Timeout(600)
    void threeMinuteSmokeIsGreen() throws Exception {
        requireDocker();
        String duration = System.getProperty("lakewire.verify.smoke.duration", "3m");
        Path output = Path.of("target", "verify-smoke");
        int code = LakeWireVerify.run(VerifyArgs.parse(new String[] {
            "--nemesis", "kill,pause,partition-pg",
            "--duration", duration,
            "--seed", "42",
            "--output", output.toString()
        }));
        assertThat(code).isZero();
        assertThat(output.resolve("verify-report.json")).exists();
        assertThat(output.resolve("verify-report.md")).exists();
        assertThat(output.resolve("verify-report.md")).content().contains("PASS");
    }

    private static void requireDocker() {
        try {
            if (!DockerClientFactory.instance().isDockerAvailable()) {
                fail("Docker is required for VerifySmokeIT. Start Docker Desktop and rerun.");
            }
        } catch (RuntimeException failure) {
            fail("Docker is required for VerifySmokeIT. Start Docker Desktop and rerun.", failure);
        }
    }
}
