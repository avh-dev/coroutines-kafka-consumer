param(
    [string]$Namespace = "ckc-observability",
    [int]$PrometheusPort = 9091,
    [int]$GrafanaPort = 3001
)

$ErrorActionPreference = "Stop"

$prometheus = Start-Process kubectl `
    -ArgumentList @("-n", $Namespace, "port-forward", "svc/ckc-prometheus", "$PrometheusPort`:9090") `
    -PassThru `
    -WindowStyle Hidden

$grafana = Start-Process kubectl `
    -ArgumentList @("-n", $Namespace, "port-forward", "svc/ckc-grafana", "$GrafanaPort`:3000") `
    -PassThru `
    -WindowStyle Hidden

Write-Host "Prometheus: http://localhost:$PrometheusPort"
Write-Host "Grafana:    http://localhost:$GrafanaPort (admin/admin)"
Write-Host "Port-forward PIDs: $($prometheus.Id), $($grafana.Id)"
