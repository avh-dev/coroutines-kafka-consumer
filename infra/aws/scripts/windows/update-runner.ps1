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
$terraformDir = Join-Path $repoRoot "infra\aws\terraform\runner"
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
            "infra/aws/assets/terraform/load-lab/main.tf" `
            "infra/aws/assets/terraform/load-lab/variables.tf" `
            "infra/aws/assets/terraform/load-lab/versions.tf" `
            "infra/aws/assets/terraform/load-lab/outputs.tf" `
            "infra/aws/assets/terraform/load-lab/terraform.tfvars.example" `
            "infra/aws/assets/terraform/load-lab/profiles" `
            "infra/aws/assets/runner" `
            "infra/aws/assets/helm" `
            "infra/aws/assets/test-definitions"
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
            "find `"$repoTarget/infra/aws/assets/runner`" -type f -name '*.sh' -exec chmod +x {} +",
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
