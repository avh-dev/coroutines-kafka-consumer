param(
    [string]$Region = "us-east-1",
    [string]$Environment = "dev",
    [switch]$SkipLoadLab,
    [switch]$SkipRunner
)

$ErrorActionPreference = "Stop"

if ($PSVersionTable.PSEdition -ne "Core") {
    $pwsh = Get-Command pwsh -ErrorAction SilentlyContinue
    if (-not $pwsh) {
        throw "destroy-all.ps1 requires PowerShell 7. Install pwsh and rerun the script."
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
$loadLabTerraformDir = Join-Path $repoRoot "infra\aws\assets\terraform\load-lab"
$runnerTerraformDir = Join-Path $repoRoot "infra\aws\terraform\runner"
$ecrTerraformDir = Join-Path $repoRoot "infra\aws\terraform\ecr"

if (-not $SkipLoadLab) {
    try {
        kubectl delete namespace ckc-loadtest --ignore-not-found=true | Out-Null
    } catch {
        Write-Host "Skipping ckc-loadtest namespace cleanup: $_"
    }

    try {
        kubectl delete namespace ckc-app --ignore-not-found=true | Out-Null
    } catch {
        Write-Host "Skipping ckc-app namespace cleanup: $_"
    }

    & terraform "-chdir=$loadLabTerraformDir" init -input=false
    & terraform "-chdir=$loadLabTerraformDir" destroy -auto-approve `
        "-var=aws_region=$Region" `
        "-var=environment=$Environment"
}

if (-not $SkipRunner) {
    & terraform "-chdir=$runnerTerraformDir" init -input=false
    & terraform "-chdir=$ecrTerraformDir" init -input=false
    & terraform "-chdir=$ecrTerraformDir" destroy -auto-approve `
        "-var=aws_region=$Region" `
        "-var=environment=$Environment"

    $runnerArgs = @(
        "-chdir=$runnerTerraformDir"
        "destroy"
        "-auto-approve"
        "-var=aws_region=$Region"
        "-var=environment=$Environment"
    )

    & terraform @runnerArgs
}
