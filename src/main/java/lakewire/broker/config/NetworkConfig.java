package lakewire.broker.config;

import lakewire.broker.network.KafkaServer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class NetworkConfig {
    @Bean
    KafkaServer kafkaServer(BrokerProperties props) {
        return new KafkaServer(props);
    }
}
