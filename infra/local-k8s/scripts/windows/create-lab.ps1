param(
    [string]$Environment = "local",
    [string]$MinikubeProfile = "minikube",
    [string]$RunnerHome = ".ckc-runner/local-k8s",
    [string]$TestDefinitionPath = "infra/shared/test-definitions/ckc-baseline-local.yaml",
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..\..")).Path
$runnerHomePath = Join-Path $repoRoot $RunnerHome
$configDir = Join-Path $runnerHomePath "config"
$contextPath = Join-Path $configDir "load-lab-$Environment.json"
$localK8sDir = Join-Path $repoRoot "infra\local-k8s"
$manifestDir = Join-Path $localK8sDir "manifests"
$localConfigDir = Join-Path $localK8sDir "config"
$helperDir = Join-Path $localK8sDir "scripts\helpers"

function Invoke-Checked {
    param([string]$File, [string[]]$Arguments)
    & $File @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$File failed with exit code $LASTEXITCODE."
    }
}

function Ensure-Namespace {
    param([string]$Name)
    $manifest = kubectl create namespace $Name --dry-run=client -o yaml
    $manifest | kubectl apply -f -
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to create namespace $Name."
    }
}

Push-Location $repoRoot
try {
    minikube -p $MinikubeProfile status | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Invoke-Checked minikube @("start", "-p", $MinikubeProfile)
    }

    Invoke-Checked kubectl @("config", "use-context", $MinikubeProfile)
    Ensure-Namespace "ckc-app"
    Ensure-Namespace "ckc-loadtest"
    Ensure-Namespace "ckc-observability"

    Invoke-Checked helm @("repo", "add", "bitnami", "https://charts.bitnami.com/bitnami", "--force-update")
    Invoke-Checked helm @("repo", "update")

    helm uninstall ckc-kafka --namespace ckc-app 2>$null
    kubectl -n ckc-app delete statefulset,svc,job,pod -l app.kubernetes.io/instance=ckc-kafka --ignore-not-found=true

    Invoke-Checked kubectl @("apply", "-f", (Join-Path $manifestDir "kafka.yaml"))
    Invoke-Checked helm @("upgrade", "--install", "ckc-redis", "bitnami/redis", "--namespace", "ckc-app", "-f", (Join-Path $localConfigDir "redis-values.yaml"))

    Invoke-Checked kubectl @("rollout", "status", "-n", "ckc-app", "deployment/ckc-kafka", "--timeout=10m")
    Invoke-Checked kubectl @("wait", "-n", "ckc-app", "--for=condition=Ready", "pod", "-l", "app.kubernetes.io/instance=ckc-redis,app.kubernetes.io/component=master", "--timeout=10m")

    $redisService = python (Join-Path $helperDir "get-service-name.py") `
        --namespace ckc-app `
        --selector app.kubernetes.io/instance=ckc-redis `
        --port 6379 `
        --preferred-token master `
        --preferred-token redis
    if ($LASTEXITCODE -ne 0) { throw "Unable to resolve Redis service." }
    $redisService = $redisService.Trim()

    Invoke-Checked python @(
        (Join-Path $repoRoot "infra\shared\test-orchestration\flush-redis.py"),
        "--host", "$redisService.ckc-app.svc.cluster.local"
    )

    Invoke-Checked python @(
        (Join-Path $repoRoot "infra\shared\test-orchestration\prepare-kafka-topics.py"),
        "--bootstrap-server", "ckc-kafka.ckc-app.svc.cluster.local:9092",
        "--replication-factor", "1",
        "--test-definition-path", $TestDefinitionPath,
        "--repo-dir", $repoRoot,
        "--admin-image", "apache/kafka:3.7.2",
        "--topics-bin", "/opt/kafka/bin/kafka-topics.sh"
    )
    Invoke-Checked kubectl @("apply", "-f", (Join-Path $manifestDir "kafka-exporter.yaml"))
    Invoke-Checked kubectl @("rollout", "status", "-n", "ckc-observability", "deployment/ckc-kafka-exporter", "--timeout=5m")

    if (-not $SkipBuild) {
        Invoke-Checked ".\gradlew.bat" @(
            ":ckc-demo:bootJar",
            ":ckc-demo-stubs:fatJar",
            ":ckc-demo-load-test:fatJar"
        )

        Invoke-Checked docker @("build", "-f", "ckc-demo/Dockerfile", "-t", "ckc-local/demo:latest", "ckc-demo")
        Invoke-Checked docker @("build", "-f", "ckc-demo-stubs/Dockerfile", "-t", "ckc-local/demo-stubs:latest", "ckc-demo-stubs")
        Invoke-Checked docker @("build", "-f", "ckc-demo-load-test/Dockerfile", "-t", "ckc-local/load-test:latest", "ckc-demo-load-test")

        Invoke-Checked minikube @("-p", $MinikubeProfile, "image", "load", "ckc-local/demo:latest")
        Invoke-Checked minikube @("-p", $MinikubeProfile, "image", "load", "ckc-local/demo-stubs:latest")
        Invoke-Checked minikube @("-p", $MinikubeProfile, "image", "load", "ckc-local/load-test:latest")
    }

    $prometheusConfigPath = Join-Path $localConfigDir "prometheus.yaml"
    $grafanaDatasourcePath = Join-Path $localConfigDir "grafana-datasource.yaml"
    kubectl -n ckc-observability create configmap ckc-prometheus-config "--from-file=prometheus.yml=$prometheusConfigPath" --dry-run=client -o yaml | kubectl apply -f -
    kubectl -n ckc-observability create configmap ckc-grafana-datasource "--from-file=prometheus.yml=$grafanaDatasourcePath" --dry-run=client -o yaml | kubectl apply -f -
    kubectl -n ckc-observability create configmap ckc-grafana-dashboard-provider --from-file=ckc.yml=infra/shared/grafana/provisioning/dashboards/ckc.yml --dry-run=client -o yaml | kubectl apply -f -
    kubectl -n ckc-observability create configmap ckc-grafana-dashboard --from-file=ckc-overview.json=infra/shared/grafana/dashboards/ckc-overview.json --dry-run=client -o yaml | kubectl apply -f -

    Invoke-Checked kubectl @("apply", "-f", (Join-Path $manifestDir "observability.yaml"))

    Invoke-Checked kubectl @("rollout", "restart", "-n", "ckc-observability", "deployment/prometheus")
    Invoke-Checked kubectl @("rollout", "status", "-n", "ckc-observability", "deployment/prometheus", "--timeout=5m")
    Invoke-Checked kubectl @("rollout", "status", "-n", "ckc-observability", "deployment/ckc-grafana", "--timeout=5m")
    Invoke-Checked kubectl @("apply", "-f", (Join-Path $manifestDir "fluent-bit-log-archive.yaml"))
    Invoke-Checked kubectl @("rollout", "status", "-n", "ckc-observability", "daemonset/ckc-fluent-bit-log-archive", "--timeout=5m")

    $kafkaService = python (Join-Path $helperDir "get-service-name.py") `
        --namespace ckc-app `
        --selector app.kubernetes.io/instance=ckc-kafka `
        --port 9092 `
        --preferred-token bootstrap `
        --preferred-token kafka
    if ($LASTEXITCODE -ne 0) { throw "Unable to resolve Kafka service." }
    $kafkaService = $kafkaService.Trim()

    New-Item -ItemType Directory -Force $configDir | Out-Null
    python (Join-Path $helperDir "write-lab-context.py") `
        --output $contextPath `
        --environment $Environment `
        --minikube-profile $MinikubeProfile `
        --kafka-service $kafkaService `
        --redis-service $redisService
    if ($LASTEXITCODE -ne 0) { throw "Unable to write local lab context." }

    Write-Host "Local k8s lab is ready."
    Write-Host "  context=$contextPath"
    Write-Host "  kafka_bootstrap=$kafkaService.ckc-app.svc.cluster.local:9092"
    Write-Host "  redis_host=$redisService.ckc-app.svc.cluster.local"
    Write-Host "  audit_log=minikube -p $MinikubeProfile ssh -- sudo tail -100 /tmp/ckc-log-archive/audit.log"
    Write-Host "  grafana: kubectl -n ckc-observability port-forward svc/ckc-grafana 3001:3000"
    Write-Host "  prometheus: kubectl -n ckc-observability port-forward svc/ckc-prometheus 9091:9090"
} finally {
    Pop-Location
}
