[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("B1", "B2", "B3", "B4", "B5", "B6_DEFERRED", "S51-B1", "S51-B2", "S51-B3", "S51-B4", "S51-B5")]
    [string]$Block,

    [Parameter(Mandatory = $true)]
    [string]$DatasetRoot,

    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")),
    [string]$EnvFile = "",
    [string]$Python = "C:\AppData\MiniConda3\envs\recweb2\python.exe",
    [switch]$Resume,
    [switch]$Yes
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$FrozenUserTokenSha256 = "269bba1c439ec13d08af42706bd4da1eb1893c32ff57a725dd886fab0c34d060"

function Test-Http200 {
    param([Parameter(Mandatory = $true)][string]$Url)
    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec 8
        return [int]$response.StatusCode -eq 200
    }
    catch {
        return $false
    }
}

function Wait-Until {
    param(
        [Parameter(Mandatory = $true)][scriptblock]$Condition,
        [Parameter(Mandatory = $true)][int]$TimeoutSeconds,
        [Parameter(Mandatory = $true)][string]$Description
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        if (& $Condition) {
            return
        }
        Start-Sleep -Seconds 5
    } while ((Get-Date) -lt $deadline)
    throw "Timed out waiting for $Description"
}

function Import-ChildEnvironment {
    param([string]$Path)
    if ([string]::IsNullOrWhiteSpace($Path)) {
        return
    }
    $resolved = (Resolve-Path -LiteralPath $Path).Path
    $allowed = @("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME", "LITE_SMOKE_USER_TOKEN")
    foreach ($line in Get-Content -LiteralPath $resolved -Encoding UTF8) {
        $trimmed = $line.Trim()
        if (-not $trimmed -or $trimmed.StartsWith("#") -or -not $trimmed.Contains("=")) {
            continue
        }
        $pair = $trimmed.Split("=", 2)
        $key = $pair[0].Trim()
        if ($allowed -notcontains $key) {
            continue
        }
        $value = $pair[1].Trim().Trim('"').Trim("'")
        [Environment]::SetEnvironmentVariable($key, $value, "Process")
    }
}

if (-not $Yes) {
    throw "Explicit -Yes is required"
}
if (-not [IO.Path]::IsPathFullyQualified($RepoRoot) -or -not [IO.Path]::IsPathFullyQualified($DatasetRoot)) {
    throw "RepoRoot and DatasetRoot must be absolute"
}

$repo = [IO.Path]::GetFullPath($RepoRoot)
$dataset = [IO.Path]::GetFullPath($DatasetRoot)
if (-not (Test-Path -LiteralPath $repo -PathType Container)) {
    throw "RepoRoot does not exist: $repo"
}
if ((Test-Path -LiteralPath $dataset) -and -not $Resume) {
    throw "DatasetRoot already exists; first-attempt-only blocks require a new root: $dataset"
}
if ($Resume) {
    $attempts = Join-Path $dataset ".qualification\$Block\.attempts"
    if (-not (Test-Path -LiteralPath $attempts -PathType Container)) {
        throw "Resume requires an existing block attempts directory: $attempts"
    }
}
if (-not (Test-Path -LiteralPath $Python -PathType Leaf)) {
    throw "Python does not exist: $Python"
}

if ([string]::IsNullOrWhiteSpace($EnvFile)) {
    $defaultEnv = Join-Path $repo ".env"
    if (Test-Path -LiteralPath $defaultEnv -PathType Leaf) {
        $EnvFile = $defaultEnv
    }
}
Import-ChildEnvironment -Path $EnvFile
if ([string]::IsNullOrWhiteSpace($env:DB_PASSWORD)) {
    throw "DB_PASSWORD must be present in the process environment or EnvFile"
}
if ([string]::IsNullOrWhiteSpace($env:LITE_SMOKE_USER_TOKEN) -or $env:LITE_SMOKE_USER_TOKEN -notmatch '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$') {
    throw "LITE_SMOKE_USER_TOKEN must be present and safe in the process environment or EnvFile"
}
$tokenBytes = [Text.Encoding]::UTF8.GetBytes($env:LITE_SMOKE_USER_TOKEN)
$tokenHash = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($tokenBytes)).ToLowerInvariant()
if ($tokenHash -ne $FrozenUserTokenSha256) {
    throw "LITE_SMOKE_USER_TOKEN differs from the frozen B1 carrier identity"
}
$env:NO_PROXY = "*"
$env:no_proxy = "*"

$activeCollectors = @(
    Get-CimInstance Win32_Process | Where-Object {
        $_.CommandLine -match 'b1_lite\.py|traditional_v2_lite\.b1_lite|chaos_k8s_runner\.py'
    }
)
if ($activeCollectors.Count -ne 0) {
    throw "Another Lite coordinator or runner is active"
}

docker info *> $null
if ($LASTEXITCODE -ne 0) {
    $dockerDesktop = "C:\Program Files\Docker\Docker\Docker Desktop.exe"
    if (-not (Test-Path -LiteralPath $dockerDesktop -PathType Leaf)) {
        throw "Docker Desktop is not running and its executable was not found"
    }
    Start-Process -FilePath $dockerDesktop -WindowStyle Hidden
    Wait-Until -TimeoutSeconds 240 -Description "Docker Engine" -Condition {
        docker info *> $null
        return $LASTEXITCODE -eq 0
    }
}
Wait-Until -TimeoutSeconds 240 -Description "Kubernetes API" -Condition {
    # 2026-08-19: /readyz (non-resource path) is 404'd by the Docker Desktop 4.82
    # host port proxy after reboot; typed API paths remain healthy. Equivalent
    # readiness via a typed path (requires auth + live apiserver).
    kubectl get --raw=/api/v1/namespaces/kube-system --request-timeout=5s *> $null
    return $LASTEXITCODE -eq 0
}

$context = (kubectl config current-context).Trim()
if ($context -ne "docker-desktop") {
    throw "Unexpected Kubernetes context: $context"
}
Wait-Until -TimeoutSeconds 300 -Description "27 ready recweb-chaos pods" -Condition {
    $document = kubectl get pods -n recweb-chaos -o json 2>$null | ConvertFrom-Json
    $pods = @($document.items)
    $notReady = @($pods | Where-Object {
        @($_.status.containerStatuses | Where-Object { $_.ready }).Count -eq 0
    })
    return $pods.Count -eq 27 -and $notReady.Count -eq 0
}

foreach ($kind in @("networkchaos", "podchaos", "stresschaos")) {
    $document = kubectl get $kind -n recweb-chaos -o json | ConvertFrom-Json
    if (@($document.items).Count -ne 0) {
        throw "Chaos residual exists: $kind"
    }
}

if (-not (Test-Http200 "http://127.0.0.1:8001/api/v1/namespaces/kube-system")) {
    $proxyOut = Join-Path $env:TEMP "recweb2-proxy8001.stdout.log"
    $proxyErr = Join-Path $env:TEMP "recweb2-proxy8001.stderr.log"
    Start-Process -FilePath "kubectl.exe" -ArgumentList @(
        "proxy", "--port=8001", "--address=0.0.0.0", "--accept-hosts=.*"
    ) -WindowStyle Hidden -RedirectStandardOutput $proxyOut -RedirectStandardError $proxyErr
    Wait-Until -TimeoutSeconds 45 -Description "kubectl proxy 8001" -Condition {
        Test-Http200 "http://127.0.0.1:8001/api/v1/namespaces/kube-system"
    }
}

$gitBash = "C:\Program Files\Git\bin\bash.exe"
if (-not (Test-Path -LiteralPath $gitBash -PathType Leaf)) {
    throw "Git Bash is required: $gitBash"
}
$portHealth = @(5000, 5004, 5005, 5009, 5011, 5013, 5014, 5017)
if (@($portHealth | Where-Object { -not (Test-Http200 "http://127.0.0.1:$_/health") }).Count -ne 0) {
    Push-Location $repo
    try {
        & $gitBash "scripts/chaos/ctk/pfwd_start.sh"
        if ($LASTEXITCODE -ne 0) {
            throw "pfwd_start.sh failed"
        }
    }
    finally {
        Pop-Location
    }
}
foreach ($port in $portHealth) {
    if (-not (Test-Http200 "http://127.0.0.1:$port/health")) {
        throw "Required service port is unhealthy: $port"
    }
}

$guardScripts = @(
    "pfwd_watchdog.sh",
    "pfwd_inventory_restarter.sh",
    "pfwd_catalog_restarter.sh",
    "pfwd_pricing_restarter.sh",
    "pfwd_user_restarter.sh"
)
$processes = @(Get-CimInstance Win32_Process)
foreach ($guard in $guardScripts) {
    if (@($processes | Where-Object { $_.CommandLine -like "*$guard*" }).Count -eq 0) {
        $path = Join-Path $repo "scripts\chaos\ctk\$guard"
        Start-Process -FilePath $gitBash -ArgumentList @($path) -WindowStyle Hidden
    }
}

foreach ($url in @(
    "http://127.0.0.1:9090/-/ready",
    "http://127.0.0.1:16686/api/services",
    "http://127.0.0.1:3100/ready"
)) {
    if (-not (Test-Http200 $url)) {
        throw "Required telemetry backend is unhealthy: $url"
    }
}

if (-not $Resume) {
    New-Item -ItemType Directory -Path $dataset -ErrorAction Stop | Out-Null
}
$stdout = Join-Path $dataset "$Block.stdout.log"
$stderr = Join-Path $dataset "$Block.stderr.log"
Push-Location $repo
try {
    & $Python -m scripts.chaos.ctk.traditional_v2_lite.b1_lite `
        --qualification-block $Block `
        --yes `
        --repo-root $repo `
        --dataset-root $dataset `
        1> $stdout 2> $stderr
    $code = $LASTEXITCODE
}
finally {
    Pop-Location
}
if ($code -ne 0) {
    throw "$Block stopped with exit code $code; evidence is preserved at $dataset"
}
Write-Output "$Block complete; stdout=$stdout; stderr=$stderr"
