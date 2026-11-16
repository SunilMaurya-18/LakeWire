package lakewire.broker;

import org.springframework.boot.SpringApplication;

public class TestLakewireBrokerApplication {

	public static void main(String[] args) {
		SpringApplication.from(LakewireBrokerApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
