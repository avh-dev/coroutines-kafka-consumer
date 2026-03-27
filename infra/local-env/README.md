Local environment for `coroutines-kafka-consumer-demo` and future demo support services.

Services:
- Kafka on `localhost:9092`
- Demo stubs on `http://localhost:18080`
- Demo stubs LT on `http://localhost:18081` when profile `lt` is enabled
- Redis on `localhost:6379`
- Prometheus on `http://localhost:9090`
- Grafana on `http://localhost:3000` (`admin` / `admin`)

Start:
```bash
docker compose -f infra/local-env/docker-compose.yml up -d
```

Start with the LT demo-stubs profile:
```bash
docker compose -f infra/local-env/docker-compose.yml --profile lt up -d
```

Stop:
```bash
docker compose -f infra/local-env/docker-compose.yml down -v
```

Created topics:
- `potion.orders.lifecycle.v1`
- `potion.cauldrons.telemetry.v1`

Demo stubs exposes:
- `POST /eta`
- `GET /health`

LT demo-stubs notes:
- uses a smaller latency profile on `localhost:18081`
- keeps the same response schema as the demo ETA model client
- avoids heavyweight mock-server templating and request journaling overhead entirely

Example application startup:
```bash
./gradlew :coroutines-kafka-consumer-demo:bootRun --args='--spring.profiles.active=ckc --demo.kafka.enabled=true'
```

When the demo app is running on `localhost:8080`, Prometheus scrapes:
- `http://host.docker.internal:8080/actuator/prometheus`

Grafana uses a pre-provisioned Prometheus datasource.
It also auto-loads the `CKC Overview` dashboard.

Spring Kafka profile:
```bash
./gradlew :coroutines-kafka-consumer-demo:bootRun --args='--spring.profiles.active=spring-kafka --demo.kafka.enabled=true'
```
