[CmdletBinding()]
param(
    [string]$DbPath = "runtime-data/axiom.sqlite",
    [string]$Python = "python",
    [string]$LockPath = "",
    [string]$LogPath = ""
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$dbInput = if ([System.IO.Path]::IsPathRooted($DbPath)) { $DbPath } else { Join-Path $root $DbPath }
$dbAbsolute = [System.IO.Path]::GetFullPath($dbInput)
Push-Location $root
try {
    $arguments = @("-m", "axiom.cli", "node-status", "--db", $dbAbsolute)
    if (-not [string]::IsNullOrWhiteSpace($LockPath)) {
        $lockInput = if ([System.IO.Path]::IsPathRooted($LockPath)) { $LockPath } else { Join-Path $root $LockPath }
        $arguments += @("--lock", ([System.IO.Path]::GetFullPath($lockInput)))
    }
    if (-not [string]::IsNullOrWhiteSpace($LogPath)) {
        $logInput = if ([System.IO.Path]::IsPathRooted($LogPath)) { $LogPath } else { Join-Path $root $LogPath }
        $arguments += @("--log", ([System.IO.Path]::GetFullPath($logInput)))
    }
    & $Python @arguments
    if ($LASTEXITCODE -ne 0) { throw "axiom node-status failed with exit code $LASTEXITCODE" }
} finally {
    Pop-Location
}
