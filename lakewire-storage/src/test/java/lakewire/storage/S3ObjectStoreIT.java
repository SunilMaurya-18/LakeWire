package lakewire.storage;

import java.net.URI;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.MinIOContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

class S3ObjectStoreIT {

    @Test
    void seaweedFsHonoursTheObjectStoreContract() {
        requireDocker();
        try (var seaweed = lakewire.testkit.LakeWireContainers.seaweedFs()) {
            seaweed.start();
            String endpoint = "http://" + seaweed.getHost() + ":" + seaweed.getMappedPort(8333);
            createBucket(endpoint, "lakewire", "lakewire", "lakewire");
            exercise(new S3StorageConfig(
                "lakewire",
                "us-east-1",
                URI.create(endpoint),
                true,
                true,
                "lakewire",
                "lakewire"
            ), "seaweedfs");
        }
    }

    @Test
    void siloHonoursTheObjectStoreContract() {
        requireDocker();
        try (MinIOContainer silo = lakewire.testkit.LakeWireContainers.silo()) {
            silo.start();
            createBucket(silo.getS3URL(), "lakewire", silo.getUserName(), silo.getPassword());
            exercise(new S3StorageConfig(
                "lakewire",
                "us-east-1",
                URI.create(silo.getS3URL()),
                true,
                true,
                silo.getUserName(),
                silo.getPassword()
            ), "silo");
        }
    }

    private static void exercise(S3StorageConfig config, String store) {
        try (S3ObjectStore objects = new S3ObjectStore(config)) {
            System.out.println("s3 conditional put probe " + store + " supported=" + objects.conditionalPutSupported());
            ObjectStoreChecks.verifyAll(objects);
        }
    }

    private static void createBucket(String endpoint, String bucket, String accessKey, String secretKey) {
        try (S3Client client = S3Client.builder()
            .endpointOverride(URI.create(endpoint))
            .region(Region.US_EAST_1)
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
            .httpClientBuilder(UrlConnectionHttpClient.builder())
            .forcePathStyle(true)
            .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
            .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
            .build()) {
            client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        }
    }

    private static void requireDocker() {
        try {
            DockerClientFactory.instance().client();
        } catch (IllegalStateException failure) {
            throw new IllegalStateException(
                "Docker is required for S3ObjectStoreIT. Start Docker and rerun.",
                failure
            );
        }
    }
}
