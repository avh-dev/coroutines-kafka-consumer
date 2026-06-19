Local environment for `ckc-demo` and future demo support services.

On Windows, run these scripts from Git Bash.
They are Bash scripts and are adapted for Git Bash path behavior.

Services:
- Redpanda Kafka API on `localhost:9092`
- Redpanda Admin API on `http://localhost:9644`
- Redis on `localhost:6379`
- Prometheus on `http://localhost:9090`
- Grafana on `http://localhost:3000` (`admin` / `admin`)
- Fluent Bit audit TCP collector on `localhost:5170` when started with the `audit` Docker Compose profile
- Demo stubs on `http://localhost:18080` when started through `scripts/stubs.sh`

Start:
```sh
demo/infra/local-dev/scripts/start.sh
```

The start script asks whether the local audit collector should be started.
For non-interactive runs, local audit collection is disabled by default and can be enabled explicitly:
```sh
LOCAL_DEV_AUDIT=true demo/infra/local-dev/scripts/start.sh
```

Start with local audit collection directly through Docker Compose:
```sh
docker compose -f demo/infra/local-dev/docker-compose.yml --profile audit up -d
```

The audit profile starts Fluent Bit.
Compact audit lines are written under `.demo-infra/local-dev/audit/live/audit.log`.
The demo app and load-test generator use `127.0.0.1:5170` by default, so no extra audit TCP settings are needed for local runs.

Run a full local test flow:
```sh
demo/infra/local-dev/scripts/run-test.sh
```

The full flow:
- recreates local Docker Compose services if they are not already running
- prompts for a demo-stubs env profile and starts stubs
- prompts for topic partition counts and recreates local topics
- prompts for a load-test env profile and starts the load-test generator
- waits until the load test finishes, or stops it early when `q` is pressed
- stops demo stubs before exiting

Start the demo application with an env profile:
```sh
demo/infra/local-dev/scripts/run-app.sh ckc
```

Stop the demo application:
```sh
demo/infra/local-dev/scripts/stop-app.sh
```

`run-app.sh` is intentionally separate from `run-test.sh`; start the app only when you want a local consumer process in the flow.

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
- `POST /brewing-registry/steps`
- `GET /settings`
- `POST /settings`
- `GET /health`

Stop demo stubs:
```sh
demo/infra/local-dev/scripts/stop-stubs.sh
```

Run the local load-test generator with an env profile:
```sh
demo/infra/local-dev/scripts/test.sh 10tps
```

Stop the local load-test generator:
```sh
demo/infra/local-dev/scripts/stop-test.sh
```

If no profile name is passed to `run-app.sh`, `stubs.sh`, or `test.sh`, the script prompts for an env file.
The first run creates editable local env files for each process under:
- `.demo-infra/local-dev/app-env/`
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
