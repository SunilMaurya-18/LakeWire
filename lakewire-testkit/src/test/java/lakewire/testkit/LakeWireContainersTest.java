package lakewire.testkit;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class LakeWireContainersTest {

    @Test
    void pinnedImageTags() {
        assertThat(LakeWireContainers.POSTGRES_IMAGE).isEqualTo("postgres:18.7-alpine");
        assertThat(LakeWireContainers.SEAWEEDFS_IMAGE).isEqualTo("chrislusf/seaweedfs:4.54");
        assertThat(LakeWireContainers.SILO_IMAGE).isEqualTo("pgsty/silo:RELEASE.2026-11-12T00-00-00Z");
        assertThat(LakeWireContainers.TOXIPROXY_IMAGE).isEqualTo("ghcr.io/shopify/toxiproxy:2.13.0");
        assertThat(LakeWireContainers.SEAWEEDFS_S3_PORT).isEqualTo(8333);
    }

    @Test
    void factoriesAcceptThePinnedImages() {
        assertThat(LakeWireContainers.postgres().getDockerImageName()).contains("postgres:18.7-alpine");
        assertThat(LakeWireContainers.seaweedFs().getExposedPorts()).contains(LakeWireContainers.SEAWEEDFS_S3_PORT);
        assertThat(LakeWireContainers.silo().getDockerImageName()).contains("pgsty/silo");
        assertThat(LakeWireContainers.toxiproxy().getDockerImageName()).contains("toxiproxy:2.13.0");
    }
}
