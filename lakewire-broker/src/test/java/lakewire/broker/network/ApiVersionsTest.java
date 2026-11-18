package lakewire.broker.network;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import lakewire.broker.config.BrokerConfig;
import lakewire.protocol.SupportedApi;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.DescribeClusterRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.DescribeClusterRequest;
import org.apache.kafka.common.requests.DescribeClusterResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ApiVersionsTest {

    private KafkaServer server;

    @BeforeEach
    void startServer() {
        server = new KafkaServer(new BrokerConfig("127.0.0.1", 0, 1_048_576));
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop();
    }

    @Test
    void apiVersionsV0ReturnsSupportedApis() throws Exception {
        try (Socket socket = new Socket("127.0.0.1", server.boundPort())) {
            socket.setSoTimeout(5_000);
            sendRawRequest(socket, (short) 18, (short) 0, 42, false);
            DataInputStream in = new DataInputStream(socket.getInputStream());

            int apis = SupportedApi.values().length;
            int size = in.readInt();
            assertThat(in.readInt()).isEqualTo(42);
            assertThat(in.readShort()).isEqualTo(Errors.NONE.code());
            assertThat(in.readInt()).isEqualTo(apis);
            assertThat(in.readShort()).isEqualTo((short) 18);
            assertThat(in.readShort()).isEqualTo((short) 0);
            assertThat(in.readShort()).isEqualTo((short) 3);
            assertThat(size).isEqualTo(10 + apis * 6);
        }
    }

    @Test
    void unsupportedVersionReturnsError35WithV0Body() throws Exception {
        try (Socket socket = new Socket("127.0.0.1", server.boundPort())) {
            socket.setSoTimeout(5_000);
            // v99 is "newer than anything we know"; such a client sends the flexible v2 header,
            // which ends with an (empty) tagged-field section.
            sendRawRequest(socket, (short) 18, (short) 99, 7, true);
            DataInputStream in = new DataInputStream(socket.getInputStream());

            int apis = SupportedApi.values().length;
            int size = in.readInt();
            assertThat(in.readInt()).isEqualTo(7);
            assertThat(in.readShort()).isEqualTo(Errors.UNSUPPORTED_VERSION.code());
            assertThat(in.readInt()).isEqualTo(apis);
            assertThat(in.readShort()).isEqualTo((short) 18);
            assertThat(in.readShort()).isEqualTo((short) 0);
            assertThat(in.readShort()).isEqualTo((short) 3);
            assertThat(size).isEqualTo(10 + apis * 6);
        }
    }

    @Test
    void flexibleV3RequestBuiltByKafkaGetsV3Response() throws Exception {
        RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, (short) 3, "lakewire-test", 99);
        ApiVersionsRequest request = new ApiVersionsRequest.Builder((short) 3).build((short) 3);

        try (KafkaTestClient client = new KafkaTestClient(server.boundPort())) {
            client.send(request.serializeWithHeader(header));
            ApiVersionsResponse response =
                (ApiVersionsResponse) AbstractResponse.parseResponse(client.receive(), header);

            assertThat(response.data().errorCode()).isEqualTo(Errors.NONE.code());
            assertThat(response.data().apiKeys()).hasSize(SupportedApi.values().length);
            ApiVersion apiVersions = response.apiVersion(ApiKeys.API_VERSIONS.id);
            assertThat(apiVersions.minVersion()).isEqualTo((short) 0);
            assertThat(apiVersions.maxVersion()).isEqualTo((short) 3);
        }
    }

    @Test
    void versionKafkaKnowsButWeDontAdvertiseFallsBackToV0() throws Exception {
        short newest = ApiKeys.API_VERSIONS.latestVersion();
        RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, newest, "lakewire-test", 5);
        ApiVersionsRequest request = new ApiVersionsRequest.Builder(newest).build(newest);

        try (KafkaTestClient client = new KafkaTestClient(server.boundPort())) {
            client.send(request.serializeWithHeader(header));
            ByteBuffer bytes = client.receive();

            ResponseHeader responseHeader = ResponseHeader.parse(bytes, (short) 0);
            ApiVersionsResponse response = ApiVersionsResponse.parse(new ByteBufferAccessor(bytes), (short) 0);
            assertThat(responseHeader.correlationId()).isEqualTo(5);
            assertThat(response.data().errorCode()).isEqualTo(Errors.UNSUPPORTED_VERSION.code());
            assertThat(response.apiVersion(ApiKeys.API_VERSIONS.id).maxVersion()).isEqualTo((short) 3);
        }
    }

    @Test
    void invalidClientSoftwareNameGetsInvalidRequest() throws Exception {
        ApiVersionsRequestData data = new ApiVersionsRequestData()
            .setClientSoftwareName("not a valid name!")
            .setClientSoftwareVersion("1.0");
        RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, (short) 3, "lakewire-test", 8);
        ApiVersionsRequest request =
            new ApiVersionsRequest.Builder(data, (short) 3, (short) 3).build((short) 3);

        try (KafkaTestClient client = new KafkaTestClient(server.boundPort())) {
            client.send(request.serializeWithHeader(header));
            ApiVersionsResponse response =
                (ApiVersionsResponse) AbstractResponse.parseResponse(client.receive(), header);
            assertThat(response.data().errorCode()).isEqualTo(Errors.INVALID_REQUEST.code());
        }
    }

    @Test
    void kafkaApiWeDontImplementGetsUnsupportedVersion() throws Exception {
        short version = ApiKeys.DESCRIBE_CLUSTER.latestVersion();
        RequestHeader header = new RequestHeader(ApiKeys.DESCRIBE_CLUSTER, version, "lakewire-test", 12);
        DescribeClusterRequest request =
            new DescribeClusterRequest.Builder(new DescribeClusterRequestData()).build(version);

        try (KafkaTestClient client = new KafkaTestClient(server.boundPort())) {
            client.send(request.serializeWithHeader(header));
            DescribeClusterResponse response =
                (DescribeClusterResponse) AbstractResponse.parseResponse(client.receive(), header);
            assertThat(response.data().errorCode()).isEqualTo(Errors.UNSUPPORTED_VERSION.code());
        }
    }

    @Test
    void unknownApiKeyClosesTheConnection() throws Exception {
        try (Socket socket = new Socket("127.0.0.1", server.boundPort())) {
            socket.setSoTimeout(5_000);
            sendRawRequest(socket, (short) 9999, (short) 0, 1, false);
            assertThat(socket.getInputStream().read()).isEqualTo(-1);
        }
    }

    private static void sendRawRequest(Socket socket, short apiKey, short version, int correlationId,
                                       boolean flexibleHeader) throws Exception {
        byte[] clientId = "test".getBytes(StandardCharsets.UTF_8);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        out.writeInt(2 + 2 + 4 + 2 + clientId.length + (flexibleHeader ? 1 : 0));
        out.writeShort(apiKey);
        out.writeShort(version);
        out.writeInt(correlationId);
        out.writeShort(clientId.length);
        out.write(clientId);
        if (flexibleHeader) {
            out.writeByte(0); // zero tagged fields
        }
        out.flush();
    }
}
