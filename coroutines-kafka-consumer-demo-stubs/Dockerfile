FROM gradle:8.10.2-jdk21 AS build
WORKDIR /workspace
COPY . .
RUN gradle :coroutines-kafka-consumer-demo-stubs:installDist --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/coroutines-kafka-consumer-demo-stubs/build/install/coroutines-kafka-consumer-demo-stubs /app
EXPOSE 8080
CMD ["bin/coroutines-kafka-consumer-demo-stubs"]
