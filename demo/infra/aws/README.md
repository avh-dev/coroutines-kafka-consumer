# AWS Infrastructure

![CKC AWS architecture](./architecture.svg)

`demo/infra/aws` is now split by responsibility instead of by historical module name.

## Layout

- `terraform/`
  Long-lived Terraform stacks managed from the local machine.
  This currently holds `runner/` and `ecr/`.

- `assets/`
  AWS-only files uploaded to the runner by `update-aws-lab`.
  This currently includes runner-side Terraform for disposable labs.

- `runner-assets/`
  Remote entrypoints that execute on the runner host.

- `../shared/`
  Test orchestration code, audit tooling, and Grafana assets reused by lab flows.

- `test-definitions/`
  AWS-owned test definitions.

- `scripts/`
  Git Bash-compatible local operator commands for creating, updating, and connecting to the runner.

## Model

- `scripts/run-experiment.sh`
  Runs one checkout-local AWS session from creation through verified teardown.

- `terraform/session-artifacts`
  Creates the encrypted, public-access-blocked S3 bucket deleted last in the session.

- `terraform/runner`
  Creates an ephemeral EC2 runner for one session. The runner stores temporary
  VictoriaMetrics, Grafana, audit, and report data while the lab exists.

- `terraform/ecr`
  Creates the only experiment workflow resources intentionally retained between
  sessions: ECR repositories for the three workload images.

- `assets/terraform`
  Defines the disposable AWS test lab. Terraform executes from the initiating
  checkout and keeps its state under `.demo-infra/aws/sessions/<session-id>`.

- `runner-assets`
  Contains remote scripts that execute on the runner and orchestrate AWS lab lifecycle.

- `helm/`
  AWS-owned Helm charts and deployment profiles for the app and stub workloads.

- `test-definitions`
  Test-run definitions that select deployment profiles and load configuration.

- `../shared/audit`
  Shared audit analysis code. AWS audit chunks can be downloaded from S3 and analyzed from any machine with Python and AWS CLI access.

## Checkout-local smoke

Run the workflow from any prepared checkout. The initiating host can be a
developer laptop, optilab, or a CI worker; no `/opt/ckc-lab` installation and no
pre-existing EC2 runner are required.

Prerequisites:

- Bash and Python 3;
- AWS CLI credentials for the target account;
- Terraform 1.8 or newer;
- Docker with Buildx when building the current checkout;
- the normal Gradle/JDK prerequisites for the demo distributions.

The default command builds linux/amd64 images, ensures the persistent ECR
repositories exist, runs the short AWS smoke definition, downloads and verifies
the result, and tears the session down:

```bash
./demo/infra/aws/scripts/run-experiment.sh run \
  --region us-east-1 \
  --test-definition demo/infra/aws/test-definitions/smoke-test.yaml
```

Reuse existing `latest` images with `--skip-build-images`. Session state and
results stay below `.demo-infra/aws/sessions`; change the root with the global
`--work-dir` option before the `run` subcommand.

SIGINT/SIGTERM and ordinary failures still enter the teardown path. If the
initiating machine loses power or the process is forcibly killed, inspect or
clean the recorded session from the same or a copied checkout-local work
directory:

```bash
./demo/infra/aws/scripts/run-experiment.sh status <session-id>
./demo/infra/aws/scripts/run-experiment.sh cleanup <session-id>
```

Cleanup never keeps AWS resources merely because the test or export failed. It
first removes Kubernetes workloads, then destroys the lab, runner, and artifact
bucket, deletes the exact EKS CloudWatch log group, and finally queries AWS for
resources still carrying the session tag as well as that untagged log group.
The local `cleanup-report.json` records that independent check. After a clean
verification, downloaded Terraform provider/module caches are removed while
the small state files, command log, lifecycle metadata, and results are kept.

## Access Model

- The ephemeral runner has no inbound security-group rules and is controlled
  through AWS Systems Manager. It currently uses a public address for outbound
  bootstrap traffic without accepting inbound connections.
- Shell access uses AWS Systems Manager Session Manager.
- Grafana, VictoriaMetrics, and the compact audit receiver run on the runner.
- Outbound internet access from the runner uses its public subnet and internet gateway; its security group still has no inbound rules.
- The runner stores lab metrics outside the disposable EKS lab. Grafana keeps the datasource name/uid `Prometheus`, backed by a VictoriaMetrics-compatible remote-write receiver on the runner.
- `create-lab` installs a Grafana Alloy agent inside EKS. Alloy discovers application and load-generator pods, Kafka exporter, and kubelet cAdvisor through the Kubernetes API; it normalizes stable job labels and remote-writes application, producer, lag, thread, and pod-resource metrics to the runner.
- AWS labs expose Kafka consumer lag through `kafka_exporter` metrics for both in-cluster Kafka and MSK. MSK profiles also start a runner-side CloudWatch exporter for managed `AWS/Kafka` lag metrics such as `MaxOffsetLag`, `SumOffsetLag`, and `EstimatedMaxTimeLag`.
- The disposable lab Terraform creates same-account VPC peering, routes, and runner security-group ingress for the remote-write path. `destroy-lab` removes that networking with the lab.

## Result layout

The downloaded result contains run metadata, the resolved test, application and
load-test logs, compact audit chunks, packet-capture diagnostics when selected,
the environment-filtered shared dashboard, runner service logs, Loki-ready log records, and a stopped VictoriaMetrics data archive.
`artifact-manifest.json` and `COMPLETE` must verify locally before the artifact
bucket can be considered safely disposable. Audit analysis runs locally only
after AWS teardown, and the final session directory contains a portable
`<run-id>-result.tar.gz`. The archive embeds `restore/open-result.sh`, Docker
Compose, anonymous read-only Grafana provisioning, and local Loki import, so viewing the metrics and logs does not require the
original repository checkout.

Open the archived metrics with:

```bash
tar -xzf <run-id>-result.tar.gz
cd <run-id>
./restore/open-result.sh
```

Stop the local containers with `./restore/close-result.sh` from the same
extracted result directory. Grafana binds to `0.0.0.0:3002` by default; pass
`./restore/open-result.sh . 3002 127.0.0.1` to restrict it to the local host.

The older `create-runner-and-ecr.sh`, `update-aws-lab.sh`, and interactive runner
entrypoints remain available for manual infrastructure development. They are
not the lifecycle used by the ephemeral smoke command.

Test definitions can use the same `diagnostic_steps` packet-capture contract as
the internal lab. For AWS runs, both `application` and `load-test` targets are
captured through Kubernetes pod exec. The test charts grant `NET_RAW` and mount
a size-limited `/captures` volume only when at least one packet capture is
configured. Completed `.pcap.gz`, JSON metadata, tcpdump stderr, index, and
summary artifacts are written under
`/opt/ckc-runner/reports/<run-id>/diagnostics/tcpdump` on the runner.
The runner then analyzes the completed captures with tshark and writes both
machine-readable and human-readable results under the sibling
`diagnostics/pcap-analysis` directory. Generated experiment reports use those
results for paired producer/consumer Kafka wire-breakdown bars.

See [terraform/README.md](terraform/README.md) and [assets/README.md](assets/README.md).

`create-lab` flushes Redis and accepts a test definition path to recreate Kafka topics from `deployment.kafka_topics` during lab setup. If omitted, AWS uses `demo/infra/aws/test-definitions/ckc-baseline.yaml`.

## Audit Analysis

AWS audit analysis is intentionally manual and location-independent. After a run has uploaded audit chunks to S3, run the shared analyzer wrapper from a local machine, the internal lab, or the AWS runner:

```sh
./demo/infra/aws/audit/analyze-s3-audit.sh s3://bucket/prefix/run-id
```

Add `--upload-summary` to copy `summary.yaml` and `analyzer-progress.log` back to the same S3 prefix.
