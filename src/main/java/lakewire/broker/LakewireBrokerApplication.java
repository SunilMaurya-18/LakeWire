package lakewire.broker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class LakewireBrokerApplication {

    public static void main(String[] args) {
        SpringApplication.run(LakewireBrokerApplication.class, args);
    }

}
