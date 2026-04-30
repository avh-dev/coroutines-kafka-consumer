param(
    [string]$Environment = "local",
    [string]$TestDefinitionPath = "infra/shared/test-definitions/ckc-baseline-local.yaml",
    [string]$RunnerHome = ".ckc-runner/local-k8s"
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..\..")).Path
$runnerHomePath = Join-Path $repoRoot $RunnerHome
$runner = Join-Path $repoRoot "infra\shared\test-orchestration\run-test.py"

Push-Location $repoRoot
try {
    python $runner `
        --environment $Environment `
        --region local `
        --repo-dir $repoRoot `
        --runner-home $runnerHomePath `
        --test-definition-path $TestDefinitionPath
} finally {
    Pop-Location
}
