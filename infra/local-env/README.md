Local environment for `coroutines-kafka-consumer-demo` and future `load-test`.

Services:
- Kafka on `localhost:9092`
- WireMock on `http://localhost:18080`
- Redis on `localhost:6379`

Start:
```bash
docker compose -f infra/local-env/docker-compose.yml up -d
```

Stop:
```bash
docker compose -f infra/local-env/docker-compose.yml down -v
```

Created topics:
- `potion.orders.lifecycle.v1`
- `potion.cauldrons.telemetry.v1`

WireMock exposes:
- `POST /eta`

Example application startup:
```bash
./gradlew :coroutines-kafka-consumer-demo:bootRun --args='--spring.profiles.active=ckc --demo.kafka.enabled=true'
```

Spring Kafka profile:
```bash
./gradlew :coroutines-kafka-consumer-demo:bootRun --args='--spring.profiles.active=spring-kafka --demo.kafka.enabled=true'
```
