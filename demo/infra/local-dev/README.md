Local environment for `ckc-demo` and future demo support services.

Services:
- Redpanda Kafka API on `localhost:9092`
- Redpanda Admin API on `http://localhost:9644`
- Demo stubs on `http://localhost:18080`
- Demo stubs LT on `http://localhost:18081` when profile `lt` is enabled
- Redis on `localhost:6379`
- Prometheus on `http://localhost:9090`
- Grafana on `http://localhost:3000` (`admin` / `admin`)

Start:
```sh
demo/infra/local-dev/scripts/start.sh
```

Recreate topics after Kafka starts:
```sh
demo/infra/local-dev/scripts/create-topics.sh --orders 6 --batches 6 --cauldrons 6
```

If no topic parameters are provided, the script prompts for all partition counts and uses `6` as the default:

```sh
demo/infra/local-dev/scripts/create-topics.sh
```

The topic script uses `docker exec ckc-local-redpanda rpk ...`, prints partition changes, deletes existing topics, and recreates:
- `order.events.v1`
- `batch.events.v1`
- `cauldron.events.v1`

To recreate topics with different partition counts:

```sh
demo/infra/local-dev/scripts/create-topics.sh --orders 4 --batches 4 --cauldrons 4
```

Start with the LT demo-stubs profile:
```sh
demo/infra/local-dev/scripts/start.sh --profile lt
```

Stop:
```sh
demo/infra/local-dev/scripts/stop.sh
```

Demo stubs exposes:
- `POST /eta`
- `POST /flavour`
- `GET /latency`
- `POST /latency`
- `GET /health`

LT demo-stubs notes:
- uses a smaller latency profile on `localhost:18081`
- keeps the same response schema as the demo ETA model client
- avoids heavyweight mock-server templating and request journaling overhead entirely

Example application startup:
```sh
./gradlew :ckc-demo:bootRun --args='--spring.profiles.active=ckc --demo.kafka.enabled=true'
```

When the demo app is running on `localhost:8080`, Prometheus scrapes:
- `http://host.docker.internal:8080/actuator/prometheus`

Grafana uses a pre-provisioned Prometheus datasource.
It also auto-loads the shared `CKC Overview` dashboard from `demo/infra/shared/grafana/dashboards`.

Spring Kafka profile:
```sh
./gradlew :ckc-demo:bootRun --args='--spring.profiles.active=spring-kafka --demo.kafka.enabled=true'
```
