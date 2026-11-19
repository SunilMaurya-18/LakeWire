package lakewire.broker.api;

import java.nio.ByteBuffer;
import lakewire.coordinator.Coordinator;
import lakewire.coordinator.CoordinatorException;
import lakewire.coordinator.ProducerIdAllocation;
import lakewire.protocol.ErrorMapping;
import lakewire.protocol.KafkaCodec;
import lakewire.protocol.KafkaRequest;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Allocates producer ids. Transactional ids are rejected (A15). */
public final class InitProducerIdHandler {

    private static final Logger log = LoggerFactory.getLogger(InitProducerIdHandler.class);

    private InitProducerIdHandler() {
    }

    public static ByteBuffer handle(KafkaRequest request, Coordinator coordinator) {
        InitProducerIdRequestData data = ((InitProducerIdRequest) request.body()).data();
        if (data.transactionalId() != null) {
            return respond(request, Errors.INVALID_REQUEST.code(), -1L, (short) -1);
        }
        if (coordinator == null) {
            return respond(request, ErrorMapping.PRODUCER_COORDINATOR_UNAVAILABLE.code(), -1L, (short) -1);
        }
        try {
            ProducerIdAllocation allocated = coordinator.initProducerId(data.producerId(), data.producerEpoch());
            if (allocated.errorCode() != Errors.NONE.code()) {
                return respond(request, allocated.errorCode(), -1L, (short) -1);
            }
            return respond(request, Errors.NONE.code(), allocated.producerId(), allocated.epoch());
        } catch (CoordinatorException failure) {
            log.warn("init producer id failed: {}", failure.toString());
            return respond(request, ErrorMapping.PRODUCER_COORDINATOR_UNAVAILABLE.code(), -1L, (short) -1);
        }
    }

    private static ByteBuffer respond(KafkaRequest request, short error, long producerId, short epoch) {
        InitProducerIdResponseData data = new InitProducerIdResponseData()
            .setThrottleTimeMs(0)
            .setErrorCode(error)
            .setProducerId(producerId)
            .setProducerEpoch(epoch);
        return KafkaCodec.encodeResponse(
            request.header(),
            request.header().apiVersion(),
            new InitProducerIdResponse(data)
        );
    }
}
