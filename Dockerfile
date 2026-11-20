FROM eclipse-temurin:21.0.13_9-jdk-noble AS build
WORKDIR /src
COPY . .
RUN chmod +x mvnw && ./mvnw -B -DskipTests package -pl lakewire-cli -am

FROM eclipse-temurin:21.0.13_9-jre-noble
RUN useradd --create-home --uid 10001 lakewire
WORKDIR /opt/lakewire
COPY --from=build /src/lakewire-cli/target/lakewire-cli-0.1.0-SNAPSHOT-bin.jar /opt/lakewire/cli.jar
COPY bin/lakewire /usr/local/bin/lakewire
RUN chmod 755 /usr/local/bin/lakewire
USER lakewire
ENTRYPOINT ["lakewire"]
CMD ["broker"]
