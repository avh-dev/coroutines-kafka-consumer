Local environment for `ckc-demo` and future demo support services.

Services:
- Redpanda Kafka API on `localhost:9092`
- Redpanda Admin API on `http://localhost:9644`
- Redis on `localhost:6379`
- Prometheus on `http://localhost:9090`
- Grafana on `http://localhost:3000` (`admin` / `admin`)
- Demo stubs on `http://localhost:18080` when started through `scripts/stubs.sh`

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

Stop:
```sh
demo/infra/local-dev/scripts/stop.sh
```

Start demo stubs with an env profile:
```sh
demo/infra/local-dev/scripts/stubs.sh baseline
```

Demo stubs exposes:
- `POST /eta`
- `POST /flavour`
- `GET /latency`
- `POST /latency`
- `GET /health`

Stop demo stubs:
```sh
demo/infra/local-dev/scripts/stop-stubs.sh
```

Run the local load-test generator with an env profile:
```sh
demo/infra/local-dev/scripts/test.sh smoke
```

Stop the local load-test generator:
```sh
demo/infra/local-dev/scripts/stop-test.sh
```

If no profile name is passed to `stubs.sh` or `test.sh`, the script prompts for an env file.
The first run creates editable local env files under:
- `.demo-infra/local-dev/stubs-env/`
- `.demo-infra/local-dev/load-test-env/`

Logs and pid files are stored under:
- `.demo-infra/local-dev/logs/`
- `.demo-infra/local-dev/pids/`

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
