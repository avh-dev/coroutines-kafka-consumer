param(
    [string]$Region = "us-east-1",
    [string]$Environment = "dev"
)

$ErrorActionPreference = "Stop"

if ($PSVersionTable.PSEdition -ne "Core") {
    $pwsh = Get-Command pwsh -ErrorAction SilentlyContinue
    if (-not $pwsh) {
        throw "build-and-push.ps1 requires PowerShell 7. Install pwsh or run the linux script from Git Bash."
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

function Invoke-DockerEcrLogin {
    param(
        [string]$Region,
        [string]$Registry
    )

    $password = (aws ecr get-login-password --region $Region).Trim()
    if (-not $password) {
        throw "Unable to get ECR login password."
    }

    $dockerProcess = New-Object System.Diagnostics.Process
    $dockerProcess.StartInfo = New-Object System.Diagnostics.ProcessStartInfo
    $dockerProcess.StartInfo.FileName = "docker"
    $dockerProcess.StartInfo.Arguments = "login --username AWS --password-stdin $Registry"
    $dockerProcess.StartInfo.RedirectStandardInput = $true
    $dockerProcess.StartInfo.RedirectStandardOutput = $true
    $dockerProcess.StartInfo.RedirectStandardError = $true
    $dockerProcess.StartInfo.UseShellExecute = $false

    try {
        [void]$dockerProcess.Start()
        $dockerProcess.StandardInput.WriteLine($password)
        $dockerProcess.StandardInput.Close()

        $stdout = $dockerProcess.StandardOutput.ReadToEnd()
        $stderr = $dockerProcess.StandardError.ReadToEnd()
        $dockerProcess.WaitForExit()

        if ($stdout) {
            Write-Host $stdout.TrimEnd()
        }

        if ($dockerProcess.ExitCode -ne 0) {
            if ($stderr) {
                throw $stderr.Trim()
            }
            throw "docker login failed with exit code $($dockerProcess.ExitCode)."
        }
    } finally {
        $dockerProcess.Dispose()
    }
}

$accountId = (aws sts get-caller-identity --query Account --output text).Trim()

if (-not $accountId) {
    throw "Unable to resolve AWS account id."
}

$registry = "$accountId.dkr.ecr.$Region.amazonaws.com"
$prefix = "$registry/ckc-load-lab-$Environment"
$demoImage = "$prefix/demo:latest"
$demoStubsImage = "$prefix/demo-stubs:latest"
$loadTestImage = "$prefix/load-test:latest"

Push-Location $repoRoot
try {
    & .\gradlew.bat `
        ":ckc-demo:installDist" `
        ":ckc-demo-stubs:installDist" `
        ":ckc-demo-load-test:installDist"

    aws ecr describe-repositories `
        --region $Region `
        --repository-names `
        "ckc-load-lab-$Environment/demo" `
        "ckc-load-lab-$Environment/demo-stubs" `
        "ckc-load-lab-$Environment/load-test" | Out-Null

    Invoke-DockerEcrLogin -Region $Region -Registry $registry

    docker build -f demo/ckc-demo/Dockerfile -t $demoImage demo/ckc-demo
    docker push $demoImage

    docker build -f demo/ckc-demo-stubs/Dockerfile -t $demoStubsImage demo/ckc-demo-stubs
    docker push $demoStubsImage

    docker build -f demo/ckc-demo-load-test/Dockerfile -t $loadTestImage demo/ckc-demo-load-test
    docker push $loadTestImage
} finally {
    Pop-Location
}

Write-Host "Pushed images:"
Write-Host "  $demoImage"
Write-Host "  $demoStubsImage"
Write-Host "  $loadTestImage"
