FROM gradle:8.10.2-jdk21 AS build
WORKDIR /workspace
COPY . .
RUN gradle :coroutines-kafka-consumer-demo-load-test:installDist --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/coroutines-kafka-consumer-demo-load-test/build/install/coroutines-kafka-consumer-demo-load-test /app
ENTRYPOINT ["bin/coroutines-kafka-consumer-demo-load-test"]
