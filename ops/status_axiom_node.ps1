[CmdletBinding()]
param(
    [string]$DbPath = "runtime-data/axiom.sqlite",
    [string]$Python = "python",
    [string]$LockPath = "",
    [string]$LogPath = "",
    [switch]$Isolated
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$dbInput = if ([System.IO.Path]::IsPathRooted($DbPath)) { $DbPath } else { Join-Path $root $DbPath }
$dbAbsolute = [System.IO.Path]::GetFullPath($dbInput)
$pidPath = "$dbAbsolute.node.pid"
$pythonExecutable = $Python
if (-not [System.IO.Path]::IsPathRooted($Python) -and ($Python.Contains("\") -or $Python.Contains("/") -or $Python.StartsWith("."))) {
    $pythonExecutable = [System.IO.Path]::GetFullPath((Join-Path $root $Python))
}

function Get-ConfiguredExecutionProfile {
    $rawProfile = [Environment]::GetEnvironmentVariable("AXIOM_EXECUTION_PROFILE", "Process")
    if ([string]::IsNullOrEmpty($rawProfile)) { return "production" }
    if ($rawProfile -eq "production" -or $rawProfile -eq "isolated") { return $rawProfile }
    throw "AXIOM_EXECUTION_PROFILE must be exactly 'production' or 'isolated'; refusing node status."
}

$ambientProfile = Get-ConfiguredExecutionProfile
$expectedProfile = if ($Isolated) { "isolated" } else { $ambientProfile }
$effectiveIsolated = $expectedProfile -eq "isolated"
if ([System.IO.Path]::GetFileNameWithoutExtension($pythonExecutable).ToLowerInvariant() -match "^pythonw(?:\d+(?:\.\d+)?)?$") {
    throw "Python launcher '$Python' cannot be used for node status because pythonw has no stdout."
}
Push-Location $root
$previousExecutionProfile = [Environment]::GetEnvironmentVariable("AXIOM_EXECUTION_PROFILE", "Process")
try {
    [Environment]::SetEnvironmentVariable("AXIOM_EXECUTION_PROFILE", $expectedProfile, "Process")
    $arguments = @("-m", "axiom.cli", "node-status", "--db", $dbAbsolute, "--pid", $pidPath)
    if (-not [string]::IsNullOrWhiteSpace($LockPath)) {
        $lockInput = if ([System.IO.Path]::IsPathRooted($LockPath)) { $LockPath } else { Join-Path $root $LockPath }
        $arguments += @("--lock", ([System.IO.Path]::GetFullPath($lockInput)))
    }
    if (-not [string]::IsNullOrWhiteSpace($LogPath)) {
        $logInput = if ([System.IO.Path]::IsPathRooted($LogPath)) { $LogPath } else { Join-Path $root $LogPath }
        $arguments += @("--log", ([System.IO.Path]::GetFullPath($logInput)))
    }
    if ($effectiveIsolated) { $arguments += "--isolated" }
    & $pythonExecutable @arguments
    if ($LASTEXITCODE -ne 0) { throw "axiom node-status failed with exit code $LASTEXITCODE" }
} finally {
    [Environment]::SetEnvironmentVariable("AXIOM_EXECUTION_PROFILE", $previousExecutionProfile, "Process")
    Pop-Location
}
