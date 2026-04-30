param(
    [string]$Region = "us-east-1",
    [string]$Environment = "dev",
    [string]$TestDefinitionPath = "infra/shared/test-definitions/ckc-baseline.yaml",
    [string]$InstanceId,
    [string]$RunnerRepoDir = "/opt/ckc-runner/assets/repo"
)

& (Join-Path $PSScriptRoot "run-test.ps1") `
    -Region $Region `
    -Environment $Environment `
    -TestDefinitionPath $TestDefinitionPath `
    -InstanceId $InstanceId `
    -RunnerRepoDir $RunnerRepoDir
exit $LASTEXITCODE
