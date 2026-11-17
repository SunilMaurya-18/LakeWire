package lakewire.protocol;

import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.RequestHeader;

public record KafkaRequest(RequestHeader header, AbstractRequest body) {

}
