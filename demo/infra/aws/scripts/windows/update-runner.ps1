param(
    [string]$Region = "us-east-1",
    [string]$InstanceId,
    [string]$RunnerAssetsDir = "/opt/ckc-runner/assets"
)

$ErrorActionPreference = "Stop"

if ($PSVersionTable.PSEdition -ne "Core") {
    $pwsh = Get-Command pwsh -ErrorAction SilentlyContinue
    if (-not $pwsh) {
        throw "update-runner.ps1 requires PowerShell 7. Install pwsh and rerun the script."
    }

    $forwardedArgs = @("-NoProfile", "-File", $PSCommandPath)
    foreach ($entry in $PSBoundParameters.GetEnumerator()) {
        $forwardedArgs += "-$($entry.Key)"
        if ($entry.Value -is [System.Management.Automation.SwitchParameter]) {
            continue
        }
        $forwardedArgs += [string]$entry.Value
    }

    & $pwsh.Source @forwardedArgs
    exit $LASTEXITCODE
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..\..")).Path
$terraformDir = Join-Path $repoRoot "demo\infra\aws\terraform\runner"
$bundleTarget = "$RunnerAssetsDir/runner-assets.tar.gz"
$repoTarget = "$RunnerAssetsDir/repo"

if (-not $InstanceId) {
    if (-not (Test-Path (Join-Path $terraformDir "terraform.tfstate"))) {
        throw "InstanceId was not provided and terraform state was not found in $terraformDir."
    }

    $InstanceId = & terraform "-chdir=$terraformDir" output -raw instance_id
}

$bundleFile = Join-Path ([System.IO.Path]::GetTempPath()) ([System.IO.Path]::GetRandomFileName() + ".tar.gz")
$commandsFile = [System.IO.Path]::GetTempFileName()

try {
    Push-Location $repoRoot
    try {
        & tar -czf $bundleFile `
            "demo/infra/aws/assets/terraform/load-lab/main.tf" `
            "demo/infra/aws/assets/terraform/load-lab/variables.tf" `
            "demo/infra/aws/assets/terraform/load-lab/versions.tf" `
            "demo/infra/aws/assets/terraform/load-lab/outputs.tf" `
            "demo/infra/aws/assets/terraform/load-lab/profiles" `
            "demo/infra/aws/assets/terraform/load-lab/terraform.tfvars.example" `
            "demo/infra/aws/runner-internal" `
            "demo/infra/shared/grafana" `
            "demo/infra/shared/helm" `
            "demo/infra/shared/test-definitions" `
            "demo/infra/shared/test-orchestration"
    } finally {
        Pop-Location
    }

    $bundleBase64 = [Convert]::ToBase64String([System.IO.File]::ReadAllBytes($bundleFile))
    $commands = @{
        commands = @(
            'set -euo pipefail',
            "mkdir -p `"$RunnerAssetsDir`"",
            "python3 - <<'PY'",
            'import base64, pathlib',
            ('data = "{0}"' -f $bundleBase64),
            ('pathlib.Path("{0}").write_bytes(base64.b64decode(data))' -f $bundleTarget),
            'PY',
            "mkdir -p `"$repoTarget`"",
            "tar -xzf `"$bundleTarget`" -C `"$repoTarget`"",
            "find `"$repoTarget/demo/infra/aws/runner-internal`" -type f -name '*.sh' -exec chmod +x {} +",
            "mkdir -p /opt/ckc-runner/observability/grafana/provisioning/dashboards /opt/ckc-runner/observability/grafana/provisioning/datasources /opt/ckc-runner/observability/grafana/dashboards",
            "cp `"$repoTarget/demo/infra/shared/grafana/provisioning/dashboards/ckc.yml`" /opt/ckc-runner/observability/grafana/provisioning/dashboards/ckc.yml",
            "cp `"$repoTarget/demo/infra/shared/grafana/provisioning/datasources/prometheus.yml`" /opt/ckc-runner/observability/grafana/provisioning/datasources/prometheus.yml",
            "cp `"$repoTarget/demo/infra/shared/grafana/dashboards/ckc-overview.json`" /opt/ckc-runner/observability/grafana/dashboards/ckc-overview.json",
            'echo synced=true',
            ('echo repo_dir={0}' -f $repoTarget)
        )
    } | ConvertTo-Json -Depth 3

    Set-Content -Path $commandsFile -Value $commands -Encoding utf8

    $commandId = aws ssm send-command `
        --region $Region `
        --instance-ids $InstanceId `
        --document-name AWS-RunShellScript `
        --comment "Sync CKC runner assets" `
        --parameters "file://$commandsFile" `
        --query "Command.CommandId" `
        --output text

    aws ssm wait command-executed --region $Region --command-id $commandId --instance-id $InstanceId
    aws ssm get-command-invocation --region $Region --command-id $commandId --instance-id $InstanceId --query "StandardOutputContent" --output text
} finally {
    Remove-Item -Force $bundleFile -ErrorAction SilentlyContinue
    Remove-Item -Force $commandsFile -ErrorAction SilentlyContinue
}
