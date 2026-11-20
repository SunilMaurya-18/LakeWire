# Spring Boot app using `assign()`

v0.1 has no consumer groups (I6 is v0.2). Point Spring Kafka at LakeWire and **assign** partitions.

```java
@Bean
ApplicationRunner runner(ConsumerFactory<String, String> factory) {
    return args -> {
        try (var consumer = factory.createConsumer()) {
            var tp = new TopicPartition("orders", 0);
            consumer.assign(List.of(tp));
            consumer.seekToBeginning(List.of(tp));
            consumer.poll(Duration.ofSeconds(2)).forEach(record ->
                System.out.println(record.offset() + " " + record.value()));
        }
    };
}
```

Producer config (idempotence on is supported):

```properties
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.producer.acks=all
spring.kafka.producer.properties.enable.idempotence=true
```

Set `client.id=...,az=a` to prefer a zone-a leader (A16). Do not set a `group.id` and do not call `subscribe()`.
