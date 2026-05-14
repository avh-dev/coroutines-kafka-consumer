param(
    [string]$Environment = "local",
    [string]$RunnerHome = ".demo-infra/runner/local-k8s"
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..\..")).Path
$contextPath = Join-Path $repoRoot (Join-Path $RunnerHome "config/load-lab-$Environment.json")

helm uninstall ckc-demo --namespace ckc-app 2>$null
helm uninstall ckc-demo-stubs --namespace ckc-app 2>$null
helm uninstall ckc-kafka --namespace ckc-app 2>$null
helm uninstall ckc-redis --namespace ckc-app 2>$null
kubectl -n ckc-app delete deployment,svc,job,pod -l app.kubernetes.io/instance=ckc-kafka --ignore-not-found=true

kubectl delete namespace ckc-loadtest --ignore-not-found=true
kubectl delete namespace ckc-observability --ignore-not-found=true
kubectl delete namespace ckc-app --ignore-not-found=true

Remove-Item -Force $contextPath -ErrorAction SilentlyContinue
