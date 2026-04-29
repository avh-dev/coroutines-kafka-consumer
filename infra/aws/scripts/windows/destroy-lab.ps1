param(
    [string]$Region = "us-east-1",
    [string]$Environment = "dev",
    [string]$ProfileName = "",
    [string]$InstanceId,
    [string]$RunnerRepoDir = "/opt/ckc-runner/assets/repo"
)

$ErrorActionPreference = "Stop"

if ($PSVersionTable.PSEdition -ne "Core") {
    $pwsh = Get-Command pwsh -ErrorAction SilentlyContinue
    if (-not $pwsh) {
        throw "destroy-lab.ps1 requires PowerShell 7. Install pwsh and rerun the script."
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

if (-not $InstanceId) {
    if (-not (Test-Path (Join-Path $terraformDir "terraform.tfstate"))) {
        throw "InstanceId was not provided and terraform state was not found in $terraformDir."
    }

    $InstanceId = & terraform "-chdir=$terraformDir" output -raw instance_id
}

& (Join-Path $PSScriptRoot "update-runner.ps1") -Region $Region -InstanceId $InstanceId | Out-Null

$tempFile = [System.IO.Path]::GetTempFileName()
try {
    $commands = @{
        commands = @(
            "set -euo pipefail",
            "RUNNER_REPO_DIR=$RunnerRepoDir",
            "cd `"`${RUNNER_REPO_DIR}`"",
            "CKC_RUNNER_REPO_DIR=`"`${RUNNER_REPO_DIR}`" ./infra/aws/assets/runner/destroy-lab.sh $Region $Environment $ProfileName"
        )
    } | ConvertTo-Json -Depth 3

    Set-Content -Path $tempFile -Value $commands -Encoding utf8

    $commandId = aws ssm send-command `
        --region $Region `
        --instance-ids $InstanceId `
        --document-name AWS-RunShellScript `
        --comment "Destroy CKC load lab" `
        --parameters "file://$tempFile" `
        --query "Command.CommandId" `
        --output text

    aws ssm wait command-executed --region $Region --command-id $commandId --instance-id $InstanceId
    aws ssm get-command-invocation --region $Region --command-id $commandId --instance-id $InstanceId --query "StandardOutputContent" --output text
} finally {
    Remove-Item -Force $tempFile -ErrorAction SilentlyContinue
}
