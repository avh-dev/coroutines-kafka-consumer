param(
    [string]$Environment = "local",
    [string]$MinikubeProfile = "minikube",
    [string]$RunnerHome = ".ckc-runner/local-k8s",
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..\..")).Path
$runnerHomePath = Join-Path $repoRoot $RunnerHome
$configDir = Join-Path $runnerHomePath "config"
$contextPath = Join-Path $configDir "load-lab-$Environment.json"

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

function Get-ServiceName {
    param(
        [string]$Namespace,
        [string]$Selector,
        [int]$Port,
        [string[]]$PreferredTokens
    )

    $services = kubectl get svc -n $Namespace -l $Selector -o json | ConvertFrom-Json
    $candidates = @(
        $services.items |
            Where-Object {
                $_.spec.clusterIP -ne "None" -and
                ($_.spec.ports | Where-Object { $_.port -eq $Port })
            } |
            ForEach-Object { $_.metadata.name }
    )

    if ($candidates.Count -eq 0) {
        throw "No service found in $Namespace for selector '$Selector' and port $Port."
    }

    foreach ($token in $PreferredTokens) {
        $match = $candidates | Where-Object { $_ -like "*$token*" } | Select-Object -First 1
        if ($match) {
            return $match
        }
    }

    return $candidates[0]
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

    @"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ckc-kafka
  namespace: ckc-app
  labels:
    app.kubernetes.io/instance: ckc-kafka
    app.kubernetes.io/name: kafka
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/instance: ckc-kafka
      app.kubernetes.io/name: kafka
  template:
    metadata:
      labels:
        app.kubernetes.io/instance: ckc-kafka
        app.kubernetes.io/name: kafka
    spec:
      containers:
        - name: kafka
          image: apache/kafka:3.7.2
          ports:
            - containerPort: 9092
            - containerPort: 9093
          env:
            - name: KAFKA_NODE_ID
              value: "1"
            - name: CLUSTER_ID
              value: MkU3OEVBNTcwNTJENDM2Qk
            - name: KAFKA_PROCESS_ROLES
              value: broker,controller
            - name: KAFKA_LISTENERS
              value: PLAINTEXT://:9092,CONTROLLER://:9093
            - name: KAFKA_ADVERTISED_LISTENERS
              value: PLAINTEXT://ckc-kafka.ckc-app.svc.cluster.local:9092
            - name: KAFKA_LISTENER_SECURITY_PROTOCOL_MAP
              value: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
            - name: KAFKA_CONTROLLER_LISTENER_NAMES
              value: CONTROLLER
            - name: KAFKA_CONTROLLER_QUORUM_VOTERS
              value: 1@localhost:9093
            - name: KAFKA_INTER_BROKER_LISTENER_NAME
              value: PLAINTEXT
            - name: KAFKA_AUTO_CREATE_TOPICS_ENABLE
              value: "true"
            - name: KAFKA_NUM_PARTITIONS
              value: "6"
            - name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR
              value: "1"
            - name: KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR
              value: "1"
            - name: KAFKA_TRANSACTION_STATE_LOG_MIN_ISR
              value: "1"
            - name: KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS
              value: "0"
          readinessProbe:
            tcpSocket:
              port: 9092
            initialDelaySeconds: 20
            periodSeconds: 10
            timeoutSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: ckc-kafka
  namespace: ckc-app
  labels:
    app.kubernetes.io/instance: ckc-kafka
    app.kubernetes.io/name: kafka
spec:
  selector:
    app.kubernetes.io/instance: ckc-kafka
    app.kubernetes.io/name: kafka
  ports:
    - name: client
      port: 9092
      targetPort: 9092
"@ | kubectl apply -f -

    $redisValues = New-TemporaryFile
    Set-Content -Path $redisValues -Value @"
architecture: standalone
auth:
  enabled: false
master:
  persistence:
    enabled: false
  resources:
    requests:
      cpu: 500m
      memory: 512Mi
    limits:
      cpu: "2"
      memory: 1Gi
  livenessProbe:
    timeoutSeconds: 15
    failureThreshold: 10
  readinessProbe:
    timeoutSeconds: 10
    failureThreshold: 10
"@
    Invoke-Checked helm @("upgrade", "--install", "ckc-redis", "bitnami/redis", "--namespace", "ckc-app", "-f", $redisValues)
    Remove-Item -Force $redisValues

    Invoke-Checked kubectl @("rollout", "status", "-n", "ckc-app", "deployment/ckc-kafka", "--timeout=10m")
    Invoke-Checked kubectl @("wait", "-n", "ckc-app", "--for=condition=Ready", "pod", "-l", "app.kubernetes.io/instance=ckc-redis,app.kubernetes.io/component=master", "--timeout=10m")

    @"
apiVersion: batch/v1
kind: Job
metadata:
  name: ckc-kafka-init
  namespace: ckc-app
  labels:
    app.kubernetes.io/instance: ckc-kafka-init
spec:
  ttlSecondsAfterFinished: 120
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: kafka-init
          image: apache/kafka:3.7.2
          command:
            - /bin/bash
            - -lc
            - |
              /opt/kafka/bin/kafka-topics.sh --bootstrap-server ckc-kafka.ckc-app.svc.cluster.local:9092 --create --if-not-exists --topic potion.orders.lifecycle.v1 --partitions 6 --replication-factor 1
              /opt/kafka/bin/kafka-topics.sh --bootstrap-server ckc-kafka.ckc-app.svc.cluster.local:9092 --create --if-not-exists --topic potion.cauldrons.telemetry.v1 --partitions 6 --replication-factor 1
"@ | kubectl apply -f -
    Invoke-Checked kubectl @("wait", "-n", "ckc-app", "--for=condition=Complete", "job/ckc-kafka-init", "--timeout=5m")

    if (-not $SkipBuild) {
        Invoke-Checked ".\gradlew.bat" @(
            ":coroutines-kafka-consumer-demo:bootJar",
            ":coroutines-kafka-consumer-demo-stubs:fatJar",
            ":coroutines-kafka-consumer-demo-load-test:fatJar"
        )

        Invoke-Checked docker @("build", "-f", "coroutines-kafka-consumer-demo/Dockerfile", "-t", "ckc-local/demo:latest", "coroutines-kafka-consumer-demo")
        Invoke-Checked docker @("build", "-f", "coroutines-kafka-consumer-demo-stubs/Dockerfile", "-t", "ckc-local/demo-stubs:latest", "coroutines-kafka-consumer-demo-stubs")
        Invoke-Checked docker @("build", "-f", "coroutines-kafka-consumer-demo-load-test/Dockerfile", "-t", "ckc-local/load-test:latest", "coroutines-kafka-consumer-demo-load-test")

        Invoke-Checked minikube @("-p", $MinikubeProfile, "image", "load", "ckc-local/demo:latest")
        Invoke-Checked minikube @("-p", $MinikubeProfile, "image", "load", "ckc-local/demo-stubs:latest")
        Invoke-Checked minikube @("-p", $MinikubeProfile, "image", "load", "ckc-local/load-test:latest")
    }

    $prometheusConfig = New-TemporaryFile
    Set-Content -Path $prometheusConfig -Value @"
global:
  scrape_interval: 5s
  evaluation_interval: 5s

scrape_configs:
  - job_name: ckc-demo-k8s
    metrics_path: /actuator/prometheus
    kubernetes_sd_configs:
      - role: pod
        namespaces:
          names:
            - ckc-app
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_name]
        regex: ckc-demo
        action: keep
      - source_labels: [__meta_kubernetes_pod_container_port_number]
        regex: "8080"
        action: keep
      - source_labels: [__meta_kubernetes_pod_name]
        target_label: pod
      - source_labels: [__meta_kubernetes_namespace]
        target_label: namespace
      - source_labels: [__meta_kubernetes_pod_node_name]
        target_label: node
"@
    kubectl -n ckc-observability create configmap ckc-prometheus-config --from-file=prometheus.yml=$prometheusConfig --dry-run=client -o yaml | kubectl apply -f -
    Remove-Item -Force $prometheusConfig

    $grafanaDatasourceConfig = New-TemporaryFile
    Set-Content -Path $grafanaDatasourceConfig -Value @"
apiVersion: 1

datasources:
  - name: Prometheus
    uid: prometheus
    type: prometheus
    access: proxy
    url: http://ckc-prometheus:9090
    isDefault: true
    editable: true
"@
    kubectl -n ckc-observability create configmap ckc-grafana-datasource --from-file=prometheus.yml=$grafanaDatasourceConfig --dry-run=client -o yaml | kubectl apply -f -
    Remove-Item -Force $grafanaDatasourceConfig
    kubectl -n ckc-observability create configmap ckc-grafana-dashboard-provider --from-file=ckc.yml=infra/shared/grafana/provisioning/dashboards/ckc.yml --dry-run=client -o yaml | kubectl apply -f -
    kubectl -n ckc-observability create configmap ckc-grafana-dashboard --from-file=ckc-overview.json=infra/shared/grafana/dashboards/ckc-overview.json --dry-run=client -o yaml | kubectl apply -f -

    @"
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ckc-prometheus
  namespace: ckc-observability
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: ckc-prometheus-discovery
rules:
  - apiGroups: [""]
    resources: ["pods", "nodes", "endpoints", "services"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: ckc-prometheus-discovery
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: ckc-prometheus-discovery
subjects:
  - kind: ServiceAccount
    name: ckc-prometheus
    namespace: ckc-observability
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prometheus
  namespace: ckc-observability
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: ckc-prometheus
  template:
    metadata:
      labels:
        app.kubernetes.io/name: ckc-prometheus
    spec:
      serviceAccountName: ckc-prometheus
      containers:
        - name: prometheus
          image: prom/prometheus:v3.3.1
          args:
            - --config.file=/etc/prometheus/prometheus.yml
            - --storage.tsdb.retention.time=2h
          ports:
            - containerPort: 9090
          volumeMounts:
            - name: config
              mountPath: /etc/prometheus
      volumes:
        - name: config
          configMap:
            name: ckc-prometheus-config
---
apiVersion: v1
kind: Service
metadata:
  name: ckc-prometheus
  namespace: ckc-observability
spec:
  selector:
    app.kubernetes.io/name: ckc-prometheus
  ports:
    - name: http
      port: 9090
      targetPort: 9090
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ckc-grafana
  namespace: ckc-observability
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: ckc-grafana
  template:
    metadata:
      labels:
        app.kubernetes.io/name: ckc-grafana
    spec:
      containers:
        - name: grafana
          image: grafana/grafana:11.6.0
          ports:
            - containerPort: 3000
          env:
            - name: GF_SECURITY_ADMIN_USER
              value: admin
            - name: GF_SECURITY_ADMIN_PASSWORD
              value: admin
          volumeMounts:
            - name: datasource
              mountPath: /etc/grafana/provisioning/datasources
            - name: dashboard-provider
              mountPath: /etc/grafana/provisioning/dashboards
            - name: dashboard
              mountPath: /var/lib/grafana/dashboards
      volumes:
        - name: datasource
          configMap:
            name: ckc-grafana-datasource
        - name: dashboard-provider
          configMap:
            name: ckc-grafana-dashboard-provider
        - name: dashboard
          configMap:
            name: ckc-grafana-dashboard
---
apiVersion: v1
kind: Service
metadata:
  name: ckc-grafana
  namespace: ckc-observability
spec:
  selector:
    app.kubernetes.io/name: ckc-grafana
  ports:
    - name: http
      port: 3000
      targetPort: 3000
"@ | kubectl apply -f -

    Invoke-Checked kubectl @("rollout", "restart", "-n", "ckc-observability", "deployment/prometheus")
    Invoke-Checked kubectl @("rollout", "status", "-n", "ckc-observability", "deployment/prometheus", "--timeout=5m")
    Invoke-Checked kubectl @("rollout", "status", "-n", "ckc-observability", "deployment/ckc-grafana", "--timeout=5m")
    Invoke-Checked kubectl @("apply", "-f", "infra/local-k8s/fluent-bit-log-archive.yaml")
    Invoke-Checked kubectl @("rollout", "status", "-n", "ckc-observability", "daemonset/ckc-fluent-bit-log-archive", "--timeout=5m")

    $kafkaService = Get-ServiceName -Namespace "ckc-app" -Selector "app.kubernetes.io/instance=ckc-kafka" -Port 9092 -PreferredTokens @("bootstrap", "kafka")
    $redisService = Get-ServiceName -Namespace "ckc-app" -Selector "app.kubernetes.io/instance=ckc-redis" -Port 6379 -PreferredTokens @("master", "redis")

    New-Item -ItemType Directory -Force $configDir | Out-Null
    $context = [ordered]@{
        environment = $Environment
        provider = "local-k8s"
        cluster_name = $MinikubeProfile
        kube_context = $MinikubeProfile
        aws_eks_update_kubeconfig = $false
        aws_registry_fallback = $false
        prometheus_bridge_enabled = $false
        cleanup_workloads = $false
        image_pull_policy = "IfNotPresent"
        kafka_mode = "kubernetes"
        kafka_bootstrap = "$kafkaService.ckc-app.svc.cluster.local:9092"
        redis_mode = "kubernetes"
        redis_host = "$redisService.ckc-app.svc.cluster.local"
        registry = "ckc-local"
        local_log_archive_path = "/tmp/ckc-log-archive"
    }
    $context | ConvertTo-Json -Depth 5 | Set-Content -Path $contextPath -Encoding utf8

    Write-Host "Local k8s lab is ready."
    Write-Host "  context=$contextPath"
    Write-Host "  kafka_bootstrap=$($context['kafka_bootstrap'])"
    Write-Host "  redis_host=$($context['redis_host'])"
    Write-Host "  audit_log=minikube -p $MinikubeProfile ssh -- sudo tail -100 /tmp/ckc-log-archive/audit.log"
    Write-Host "  grafana: kubectl -n ckc-observability port-forward svc/ckc-grafana 3000:3000"
    Write-Host "  prometheus: kubectl -n ckc-observability port-forward svc/ckc-prometheus 9090:9090"
} finally {
    Pop-Location
}
