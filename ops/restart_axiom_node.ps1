[CmdletBinding()]
param(
    [string]$DbPath = "runtime-data/axiom_phase3.sqlite",
    [double]$IntervalSeconds = 60,
    [ValidateSet("public", "synthetic", "disabled")]
    [string]$CryptoSource = "public",
    [ValidateRange(1, 100)]
    [int]$Depth = 20,
    [ValidateRange(1, 1000)]
    [int]$MaxMarkets = 100,
    [string]$LockPath = "",
    [string]$LogPath = "",
    [string]$Python = "python"
)

$ErrorActionPreference = "Stop"
& (Join-Path $PSScriptRoot "stop_axiom_node.ps1") -DbPath $DbPath -LockPath $LockPath
& (Join-Path $PSScriptRoot "start_axiom_node.ps1") -DbPath $DbPath -IntervalSeconds $IntervalSeconds -CryptoSource $CryptoSource -Depth $Depth -MaxMarkets $MaxMarkets -LockPath $LockPath -LogPath $LogPath -Python $Python
