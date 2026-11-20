package lakewire.admin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
public class LakewireAdminApplication {

    public static void main(String[] args) {
        SpringApplication.run(LakewireAdminApplication.class, args);
    }
}
