[CmdletBinding()]
param(
    [string]$DbPath = "runtime-data/axiom.sqlite",
    [double]$IntervalSeconds = 60,
    [ValidateSet("public", "synthetic", "disabled")]
    [string]$CryptoSource = "public",
    [ValidateRange(1, 100)]
    [int]$Depth = 20,
    [ValidateRange(1, 1000)]
    [int]$MaxMarkets = 100,
    [string]$LogPath = "",
    [int]$GracefulTimeoutSeconds = 120,
    [switch]$Isolated,
    [string]$Python = "python"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$dbInput = if ([System.IO.Path]::IsPathRooted($DbPath)) { $DbPath } else { Join-Path $root $DbPath }
$dbAbsolute = [System.IO.Path]::GetFullPath($dbInput)
$lockPath = "$dbAbsolute.lock"
$pidPath = "$dbAbsolute.node.pid"

function Get-ConfiguredExecutionProfile {
    $rawProfile = [Environment]::GetEnvironmentVariable("AXIOM_EXECUTION_PROFILE", "Process")
    if ([string]::IsNullOrEmpty($rawProfile)) { return "production" }
    if ($rawProfile -eq "production" -or $rawProfile -eq "isolated") { return $rawProfile }
    throw "AXIOM_EXECUTION_PROFILE must be exactly 'production' or 'isolated'; refusing restart."
}

function Get-CommandTokens([string]$CommandLine) {
    $tokens = @()
    foreach ($match in [regex]::Matches($CommandLine, '"([^"]*)"|''([^'']*)''|(\S+)')) {
        $value = $match.Groups[1].Value
        if (-not $value) { $value = $match.Groups[2].Value }
        if (-not $value) { $value = $match.Groups[3].Value }
        $tokens += $value
    }
    return $tokens
}

function Get-NodeProfile([string]$CommandLine, [string]$ExpectedDb, [string]$PidMarkerPath) {
    if (-not $CommandLine) { return "" }
    $tokens = @(Get-CommandTokens $CommandLine)
    if ($tokens.Count -eq 0) { return "" }
    $executable = $tokens[0].Replace("\", "/").Split("/")[-1].ToLowerInvariant()
    if ($executable -match "^pythonw(?:\d+(?:\.\d+)?)?(?:\.exe)?$") { return "" }
    if ($executable -in @("axiom", "axiom.exe")) {
        $commandIndex = 1
    } elseif ($executable -in @("py", "py.exe") -or $executable -match "^python(?:\d+(?:\.\d+)?)?(?:\.exe)?$") {
        if ($tokens.Count -lt 4 -or $tokens[1].ToLowerInvariant() -ne "-m" -or $tokens[2].ToLowerInvariant() -ne "axiom.cli") { return "" }
        $commandIndex = 3
    } else {
        return ""
    }
    if ($tokens.Count -le $commandIndex -or $tokens[$commandIndex].ToLowerInvariant() -notin @("node-run", "run-research-node")) { return "" }
    $actualDb = $null
    for ($index = $commandIndex + 1; $index -lt $tokens.Count; $index++) {
        $token = $tokens[$index]
        if ($token.ToLowerInvariant() -eq "--db" -and $index + 1 -lt $tokens.Count) {
            $actualDb = $tokens[$index + 1]
            break
        }
        if ($token.ToLowerInvariant().StartsWith("--db=")) {
            $actualDb = $token.Substring(5)
            break
        }
    }
    if (-not $actualDb) { return "" }
    try {
        if (-not [StringComparer]::OrdinalIgnoreCase.Equals([System.IO.Path]::GetFullPath($actualDb), $ExpectedDb)) { return "" }
    } catch {
        return ""
    }
    $isolatedFlag = $tokens | Where-Object { $_.ToLowerInvariant() -eq "--isolated" }
    if (-not [string]::IsNullOrWhiteSpace($PidMarkerPath) -and (Test-Path -LiteralPath $PidMarkerPath)) {
        try {
            $markerLines = @(Get-Content -LiteralPath $PidMarkerPath)
            if ($markerLines.Count -ge 4) {
                $persistedProfile = [string]$markerLines[3]
                if ($persistedProfile -in @("production", "isolated")) {
                    return $persistedProfile
                }
            }
        } catch {
            return ""
        }
    }
    if ($tokens | Where-Object { $_.ToLowerInvariant() -eq "--isolated" }) { return "isolated" }
    return "production"
}

function Get-NodeCommandLine([int]$ProcessId) {
    try {
        return [string](Get-CimInstance Win32_Process -Filter "ProcessId=$ProcessId" -ErrorAction Stop).CommandLine
    } catch {
        return ""
    }
}

function Get-ProcessSafe([int]$ProcessId, [ref]$QueryFailed) {
    $QueryFailed.Value = $false
    try {
        return Get-Process -Id $ProcessId -ErrorAction Stop
    } catch {
        $category = [string]$_.CategoryInfo.Category
        if ($category -eq "ObjectNotFound" -or $_.Exception.Message -match "Cannot find|No process") { return $null }
        $QueryFailed.Value = $true
        return $null
    }
}

function Get-FilePid([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path)) { return 0 }
    try {
        $lines = @(Get-Content -LiteralPath $Path)
        return [int]$lines[0]
    } catch {
        return 0
    }
}


$ambientProfile = Get-ConfiguredExecutionProfile
$effectiveIsolated = if ($Isolated) { $true } else { $ambientProfile -eq "isolated" }
$profileFromRunningNode = $false
$stopRequiresIsolatedFlag = $false

if (Test-Path -LiteralPath $lockPath) {
    $lockPid = Get-FilePid $lockPath
    if ($lockPid -le 0) { throw "Axiom node lock $lockPath exists but has no valid owner; refusing restart." }
    $lockQueryFailed = $false
    $lockProcess = Get-ProcessSafe $lockPid ([ref]$lockQueryFailed)
    if ($lockQueryFailed) { throw "Cannot establish ownership of Axiom node lock $lockPath; refusing restart." }
    if ($lockProcess) {
        # The lock owner is checked here for exact PID and node command/profile
        # identity.  The named-splat stop below is the lifecycle identity
        # authority: it revalidates the PID marker's persisted start time and
        # command before touching the process, rejecting PID reuse without a
        # second timestamp parser that can disagree immediately after launch.
        $lockCommand = Get-NodeCommandLine $lockPid
        $runningProfile = Get-NodeProfile $lockCommand $dbAbsolute $pidPath
        if ([string]::IsNullOrWhiteSpace($runningProfile)) {
            throw "Axiom node lock $lockPath belongs to a process that is not this node; refusing restart."
        }
        if ($Isolated -and $runningProfile -ne "isolated") {
            throw "Axiom node is running in production; refusing an isolated restart request."
        }
        $effectiveIsolated = $runningProfile -eq "isolated"
        $profileFromRunningNode = $true
        $stopRequiresIsolatedFlag = $null -ne (
            @(Get-CommandTokens $lockCommand) |
            Where-Object { $_.ToLowerInvariant() -eq "--isolated" } |
            Select-Object -First 1
        )
    }
}

if (-not $profileFromRunningNode -and (Test-Path -LiteralPath $pidPath)) {
    $candidatePid = Get-FilePid $pidPath
    if ($candidatePid -gt 0) {
        $candidateQueryFailed = $false
        $candidateProcess = Get-ProcessSafe $candidatePid ([ref]$candidateQueryFailed)
        if ($candidateQueryFailed) { throw "Cannot establish identity for PID file $pidPath; refusing restart." }
        if ($candidateProcess) {
            $candidateCommand = Get-NodeCommandLine $candidatePid
            $runningProfile = Get-NodeProfile $candidateCommand $dbAbsolute $pidPath
            if ([string]::IsNullOrWhiteSpace($runningProfile)) {
                throw "PID file $pidPath belongs to a process that is not this node; refusing restart."
            }
            if ($Isolated -and $runningProfile -ne "isolated") {
                throw "Axiom node is running in production; refusing an isolated restart request."
            }
            $effectiveIsolated = $runningProfile -eq "isolated"
            $stopRequiresIsolatedFlag = $null -ne (
                @(Get-CommandTokens $candidateCommand) |
                Where-Object { $_.ToLowerInvariant() -eq "--isolated" } |
                Select-Object -First 1
            )
        }
    }
}

$effectiveProfile = if ($effectiveIsolated) { "isolated" } else { "production" }
$previousExecutionProfile = [Environment]::GetEnvironmentVariable("AXIOM_EXECUTION_PROFILE", "Process")
try {
    # The child start script treats an omitted switch as ambient profile.
    # Set that ambient value to the verified profile so production cannot be
    # upgraded by an isolated caller, or isolated downgraded by a default.
    [Environment]::SetEnvironmentVariable("AXIOM_EXECUTION_PROFILE", $effectiveProfile, "Process")

    $stopParameters = @{
        DbPath = [string]$DbPath
        GracefulTimeoutSeconds = [int]$GracefulTimeoutSeconds
    }
    if ($stopRequiresIsolatedFlag) { $stopParameters["Isolated"] = $true }
    & (Join-Path $PSScriptRoot "stop_axiom_node.ps1") @stopParameters

    $startParameters = @{
        DbPath = [string]$DbPath
        IntervalSeconds = [double]$IntervalSeconds
        CryptoSource = [string]$CryptoSource
        Depth = [int]$Depth
        MaxMarkets = [int]$MaxMarkets
        LogPath = [string]$LogPath
        Python = [string]$Python
    }
    if ($effectiveIsolated) { $startParameters["Isolated"] = $true }
    & (Join-Path $PSScriptRoot "start_axiom_node.ps1") @startParameters
} finally {
    [Environment]::SetEnvironmentVariable("AXIOM_EXECUTION_PROFILE", $previousExecutionProfile, "Process")
}
