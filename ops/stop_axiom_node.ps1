[CmdletBinding()]
param(
    [string]$DbPath = "runtime-data/axiom_phase3.sqlite",
    [int]$GracefulTimeoutSeconds = 120,
    [string]$LockPath = "",
    [switch]$Force
)

$ErrorActionPreference = "Stop"
$customLockPath = $LockPath
if ($GracefulTimeoutSeconds -le 0) { throw "GracefulTimeoutSeconds must be positive." }
$root = Split-Path -Parent $PSScriptRoot
$dbInput = if ([System.IO.Path]::IsPathRooted($DbPath)) { $DbPath } else { Join-Path $root $DbPath }
$dbAbsolute = [System.IO.Path]::GetFullPath($dbInput)
$pidPath = "$dbAbsolute.node.pid"
$lockInput = if ([string]::IsNullOrWhiteSpace($LockPath)) { "$dbAbsolute.lock" } elseif ([System.IO.Path]::IsPathRooted($LockPath)) { $LockPath } else { Join-Path $root $LockPath }
$lockPath = [System.IO.Path]::GetFullPath($lockInput)
$stopPath = "$dbAbsolute.stop"

function Get-NodeCommandLine([int]$ProcessId) {
    try {
        return [string](Get-CimInstance Win32_Process -Filter "ProcessId=$ProcessId" -ErrorAction Stop).CommandLine
    } catch {
        return ""
    }
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

function Test-NodeCommand([string]$CommandLine, [string]$ExpectedDb) {
    if (-not $CommandLine) { return $false }
    $tokens = @(Get-CommandTokens $CommandLine)
    if ($tokens.Count -eq 0) { return $false }
    $executable = $tokens[0].Replace("\", "/").Split("/")[-1].ToLowerInvariant()
    if ($executable -in @("axiom", "axiom.exe")) {
        $commandIndex = 1
    } elseif ($executable -in @("py", "py.exe") -or $executable -match "^pythonw?(?:\d+(?:\.\d+)?)?(?:\.exe)?$") {
        if ($tokens.Count -lt 4 -or $tokens[1].ToLowerInvariant() -ne "-m" -or $tokens[2].ToLowerInvariant() -ne "axiom.cli") { return $false }
        $commandIndex = 3
    } else {
        return $false
    }
    if ($tokens.Count -le $commandIndex -or $tokens[$commandIndex].ToLowerInvariant() -notin @("node-run", "run-research-node")) { return $false }
    $actual = $null
    for ($index = $commandIndex + 1; $index -lt $tokens.Count; $index++) {
        $token = $tokens[$index]
        if ($token.ToLowerInvariant() -eq "--db" -and $index + 1 -lt $tokens.Count) {
            $actual = $tokens[$index + 1]
            break
        }
        if ($token.ToLowerInvariant().StartsWith("--db=")) {
            $actual = $token.Substring(5)
            break
        }
    }
    if (-not $actual) { return $false }
    try {
        return [StringComparer]::OrdinalIgnoreCase.Equals([System.IO.Path]::GetFullPath($actual), $ExpectedDb)
    } catch {
        return $false
    }
}

function Test-ProcessIdentity($Process, [datetime]$StartTime, [string]$ExpectedDb) {
    if (-not $Process) { return $false }
    try {
        $Process.Refresh()
        $currentStart = ([datetime]$Process.StartTime).ToUniversalTime()
        if ($Process.HasExited -or $currentStart.Ticks -ne $StartTime.ToUniversalTime().Ticks) { return $false }
    } catch {
        return $false
    }
    return Test-NodeCommand (Get-NodeCommandLine $Process.Id) $ExpectedDb
}
function Get-ProcessSafe([int]$ProcessId, [ref]$QueryFailed) {
    $QueryFailed.Value = $false
    try {
        return Get-Process -Id $ProcessId -ErrorAction Stop
    } catch {
        $category = [string]$_.CategoryInfo.Category
        if ($category -eq "ObjectNotFound" -or $_.Exception.Message -match "Cannot find|No process") {
            return $null
        }
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

function Get-FileStartTime([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path)) { return [datetime]::MinValue }
    try {
        $lines = @(Get-Content -LiteralPath $Path)
        if ($lines.Count -lt 2) { return [datetime]::MinValue }
        return [datetime]::new([long]$lines[1], [DateTimeKind]::Utc)
    } catch {
        return [datetime]::MinValue
    }
}


$nodePid = Get-FilePid $pidPath
if ([string]::IsNullOrWhiteSpace($customLockPath) -and $nodePid -gt 0) {
    $processQueryFailed = $false
    $candidateProcess = Get-ProcessSafe $nodePid ([ref]$processQueryFailed)
    if ($processQueryFailed) {
        throw "Cannot establish identity for PID $nodePid; refusing to infer its lock path."
    }
    if ($candidateProcess) {
        $tokens = @(Get-CommandTokens (Get-NodeCommandLine $nodePid))
        for ($index = 0; $index -lt $tokens.Count; $index++) {
            if ($tokens[$index].ToLowerInvariant() -eq "--lock" -and $index + 1 -lt $tokens.Count) {
                $lockCandidate = $tokens[$index + 1]
                $lockInput = if ([System.IO.Path]::IsPathRooted($lockCandidate)) { $lockCandidate } else { Join-Path $root $lockCandidate }
                $lockPath = [System.IO.Path]::GetFullPath($lockInput)
                break
            }
            if ($tokens[$index].ToLowerInvariant().StartsWith("--lock=")) {
                $lockCandidate = $tokens[$index].Substring(7)
                $lockInput = if ([System.IO.Path]::IsPathRooted($lockCandidate)) { $lockCandidate } else { Join-Path $root $lockCandidate }
                $lockPath = [System.IO.Path]::GetFullPath($lockInput)
                break
            }
        }
    }
}
$recordedStart = Get-FileStartTime $pidPath
if ($nodePid -le 0) {
    $lockPid = Get-FilePid $lockPath
    if ($lockPid -gt 0) {
        $processQueryFailed = $false
        $lockProcess = Get-ProcessSafe $lockPid ([ref]$processQueryFailed)
        if ($processQueryFailed) {
            throw "Cannot establish ownership of Axiom node lock $lockPath; refusing cleanup."
        }
        if ($lockProcess) {
            if (Test-NodeCommand (Get-NodeCommandLine $lockPid) $dbAbsolute) {
                throw "Axiom node lock $lockPath belongs to live PID $lockPid; refusing to remove it."
            }
            throw "Axiom node lock $lockPath references live PID $lockPid; refusing to remove it."
        }
        if ((Get-FilePid $lockPath) -eq $lockPid) {
            $staleLockPath = "$lockPath.stale.$([guid]::NewGuid().ToString('N'))"
            try {
                Move-Item -LiteralPath $lockPath -Destination $staleLockPath -ErrorAction Stop
                Remove-Item -LiteralPath $staleLockPath -Force -ErrorAction Stop
            } catch {
                throw "Axiom node lock $lockPath changed while checking its stale owner; refusing cleanup."
            }
        }
    }
    if (Test-Path -LiteralPath $stopPath) {
        $staleStopPath = "$stopPath.stale.$([guid]::NewGuid().ToString('N'))"
        try {
            Move-Item -LiteralPath $stopPath -Destination $staleStopPath -ErrorAction Stop
            Remove-Item -LiteralPath $staleStopPath -Force -ErrorAction Stop
        } catch {
            throw "Stop marker $stopPath changed while checking its stale owner; refusing cleanup."
        }
    }
    Write-Output "Axiom node is not running."
    return
}

$processQueryFailed = $false
$process = Get-ProcessSafe $nodePid ([ref]$processQueryFailed)
if ($processQueryFailed) {
    throw "Cannot establish process identity for PID $nodePid; refusing to stop it."
}
if ($process) {
    if (-not (Test-NodeCommand (Get-NodeCommandLine $nodePid) $dbAbsolute)) {
        throw "PID file $pidPath does not identify this Axiom node; refusing to stop PID $nodePid."
    }
    if ($recordedStart.Ticks -eq [datetime]::MinValue.Ticks) {
        throw "PID file $pidPath has no persisted process start time; refusing to stop PID $nodePid."
    }
    try { $processStartTime = ([datetime]$process.StartTime).ToUniversalTime() } catch {
        throw "Cannot establish process identity for PID $nodePid; refusing to stop it."
    }
    if ($processStartTime.Ticks -ne $recordedStart.Ticks -or -not (Test-ProcessIdentity $process $recordedStart $dbAbsolute)) {
        throw "PID $nodePid changed identity; refusing to stop it."
    }
    $lockMarker = ""
    try { $lockMarker = (Get-Content -LiteralPath $lockPath -Raw).Trim() } catch {}
    if ([string]::IsNullOrWhiteSpace($lockMarker) -or (Get-FilePid $lockPath) -ne $nodePid) {
        throw "Axiom node lock $lockPath changed before stop request; refusing to stop PID $nodePid."
    }
    Set-Content -LiteralPath $stopPath -Value $lockMarker -NoNewline
    $deadline = (Get-Date).AddSeconds($GracefulTimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if (-not (Test-ProcessIdentity $process $processStartTime $dbAbsolute)) { break }
        Start-Sleep -Milliseconds 250
    }
    try { $process.Refresh() } catch {}
    $stillRunning = $false
    try { $stillRunning = -not $process.HasExited } catch {}
    if ($stillRunning) {
        if (-not $Force) {
            throw "Axiom node PID $nodePid did not stop within $GracefulTimeoutSeconds seconds; it remains marked for graceful shutdown. Rerun with -Force only if required."
        }
        if (-not (Test-ProcessIdentity $process $processStartTime $dbAbsolute)) {
            throw "PID $nodePid changed identity; refusing forced stop."
        }
        Stop-Process -InputObject $process -Force -ErrorAction Stop
        Start-Sleep -Milliseconds 250
        try { $process.Refresh() } catch {}
        try {
            if (-not $process.HasExited) { throw "Axiom node PID $nodePid did not exit after forced shutdown." }
        } catch {
            throw "Axiom node PID $nodePid did not exit after forced shutdown."
        }
    }
    Write-Output "Axiom node stopped (PID $nodePid)."
} else {
    Write-Output "Axiom node process $nodePid was already stopped."
}

# Do not delete a file that a reused PID now owns.
$processQueryFailed = $false
$currentProcess = Get-ProcessSafe $nodePid ([ref]$processQueryFailed)
if ($processQueryFailed) {
    throw "Cannot revalidate PID $nodePid; refusing stale-file cleanup."
}
if ($currentProcess) {
    throw "PID $nodePid was reused; refusing stale-file cleanup."
}
if (Test-Path -LiteralPath $pidPath) {
    if ((Get-FilePid $pidPath) -eq $nodePid) {
        $stalePidPath = "$pidPath.stale.$([guid]::NewGuid().ToString('N'))"
        try {
            Move-Item -LiteralPath $pidPath -Destination $stalePidPath -ErrorAction Stop
            Remove-Item -LiteralPath $stalePidPath -Force -ErrorAction Stop
        } catch {
            throw "PID file $pidPath changed during stale-file cleanup; refusing cleanup."
        }
    }
}
if (Test-Path -LiteralPath $lockPath) {
    if ((Get-FilePid $lockPath) -eq $nodePid) {
        $staleLockPath = "$lockPath.stale.$([guid]::NewGuid().ToString('N'))"
        try {
            Move-Item -LiteralPath $lockPath -Destination $staleLockPath -ErrorAction Stop
            Remove-Item -LiteralPath $staleLockPath -Force -ErrorAction Stop
        } catch {
            throw "Axiom node lock $lockPath changed during stale-file cleanup; refusing cleanup."
        }
    }
}
if (Test-Path -LiteralPath $stopPath) {
    $markerPid = Get-FilePid $stopPath
    if ($markerPid -le 0 -or $markerPid -eq $nodePid) {
        $staleStopPath = "$stopPath.stale.$([guid]::NewGuid().ToString('N'))"
        try {
            Move-Item -LiteralPath $stopPath -Destination $staleStopPath -ErrorAction Stop
            Remove-Item -LiteralPath $staleStopPath -Force -ErrorAction Stop
        } catch {
            throw "Stop marker $stopPath changed during cleanup; refusing cleanup."
        }
    }
}
