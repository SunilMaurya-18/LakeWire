package lakewire.broker.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix="lakewire.broker")
public record BrokerProperties (

        @NotBlank String host,
        @Min(0) @Max(65535) int port,
        @Min(1024) int maxFrameBytes
)

{}
