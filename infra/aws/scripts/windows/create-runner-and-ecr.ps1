param(
    [string]$Region = "us-east-1",
    [string]$Environment = "dev"
)

$ErrorActionPreference = "Stop"

if ($PSVersionTable.PSEdition -ne "Core") {
    $pwsh = Get-Command pwsh -ErrorAction SilentlyContinue
    if (-not $pwsh) {
        throw "create-runner-and-ecr.ps1 requires PowerShell 7. Install pwsh and rerun the script."
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
$runnerTerraformDir = Join-Path $repoRoot "infra\aws\terraform\runner"
$ecrTerraformDir = Join-Path $repoRoot "infra\aws\terraform\ecr"

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

function Sync-RunnerAssets {
    param(
        [string]$Region,
        [string]$InstanceId
    )

    & (Join-Path $PSScriptRoot "update-runner.ps1") -Region $Region -InstanceId $InstanceId
}

if (-not (Test-Path (Join-Path $runnerTerraformDir "terraform.tfvars"))) {
    Copy-Item (Join-Path $runnerTerraformDir "terraform.tfvars.example") (Join-Path $runnerTerraformDir "terraform.tfvars")
}

if (-not (Test-Path (Join-Path $ecrTerraformDir "terraform.tfvars"))) {
    Copy-Item (Join-Path $ecrTerraformDir "terraform.tfvars.example") (Join-Path $ecrTerraformDir "terraform.tfvars")
}

& terraform "-chdir=$runnerTerraformDir" init
& terraform "-chdir=$runnerTerraformDir" apply -auto-approve `
    "-var=aws_region=$Region" `
    "-var=environment=$Environment"

$instanceId = & terraform "-chdir=$runnerTerraformDir" output -raw instance_id
aws ec2 wait instance-status-ok --region $Region --instance-ids $instanceId
Wait-RunnerSsmOnline -Region $Region -InstanceId $instanceId
Wait-RunnerBootstrap -Region $Region -InstanceId $instanceId
Sync-RunnerAssets -Region $Region -InstanceId $instanceId

& terraform "-chdir=$ecrTerraformDir" init
& terraform "-chdir=$ecrTerraformDir" apply -auto-approve `
    "-var=aws_region=$Region" `
    "-var=environment=$Environment"

& terraform "-chdir=$runnerTerraformDir" output
& terraform "-chdir=$ecrTerraformDir" output
