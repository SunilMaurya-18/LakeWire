package lakewire.protocol;

import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.RequestHeader;

/** One decoded request: Kafka's header plus Kafka's typed request body. */
public record KafkaRequest(RequestHeader header, AbstractRequest body) {
}
