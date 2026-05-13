param(
    [string]$Region = "us-east-1",
    [string]$InstanceId
)

$ErrorActionPreference = "Stop"

if ($PSVersionTable.PSEdition -ne "Core") {
    $pwsh = Get-Command pwsh -ErrorAction SilentlyContinue
    if (-not $pwsh) {
        throw "start-runner.ps1 requires PowerShell 7. Install pwsh and rerun the script."
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

function Wait-RunnerSsmOnline {
    param(
        [string]$Region,
        [string]$InstanceId
    )

    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        $status = aws ssm describe-instance-information `
            --region $Region `
            --filters "Key=InstanceIds,Values=$InstanceId" `
            --query "InstanceInformationList[0].PingStatus" `
            --output text 2>$null
        if ($status -eq "Online") {
            return
        }
        Start-Sleep -Seconds 10
    }

    throw "Runner instance did not become SSM-online in time: $InstanceId"
}

function Wait-RunnerBootstrap {
    param(
        [string]$Region,
        [string]$InstanceId
    )

    $commandId = aws ssm send-command `
        --region $Region `
        --instance-ids $InstanceId `
        --document-name AWS-RunShellScript `
        --comment "Wait for CKC runner bootstrap" `
        --parameters 'commands=["cloud-init status --wait","systemctl is-active ckc-runner-observability.service","docker ps --format {{.Names}} | grep -q prometheus","docker ps --format {{.Names}} | grep -q grafana"]' `
        --query "Command.CommandId" `
        --output text

    aws ssm wait command-executed --region $Region --command-id $commandId --instance-id $InstanceId
}

if (-not $InstanceId) {
    if (-not (Test-Path (Join-Path $terraformDir "terraform.tfstate"))) {
        throw "InstanceId was not provided and terraform state was not found in $terraformDir."
    }

    $InstanceId = & terraform "-chdir=$terraformDir" output -raw instance_id
}

aws ec2 start-instances --instance-ids $InstanceId --region $Region | Out-Null
aws ec2 wait instance-running --instance-ids $InstanceId --region $Region
aws ec2 wait instance-status-ok --instance-ids $InstanceId --region $Region
Wait-RunnerSsmOnline -Region $Region -InstanceId $InstanceId
Wait-RunnerBootstrap -Region $Region -InstanceId $InstanceId
Write-Host "Runner instance is running: $InstanceId"
