param(
    [string]$Region = "us-east-1",
    [string]$InstanceId
)

$ErrorActionPreference = "Stop"

if ($PSVersionTable.PSEdition -ne "Core") {
    $pwsh = Get-Command pwsh -ErrorAction SilentlyContinue
    if (-not $pwsh) {
        throw "stop-runner.ps1 requires PowerShell 7. Install pwsh and rerun the script."
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

aws ec2 stop-instances --instance-ids $InstanceId --region $Region | Out-Null
aws ec2 wait instance-stopped --instance-ids $InstanceId --region $Region
Write-Host "Runner instance is stopped: $InstanceId"
