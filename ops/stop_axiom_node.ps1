[CmdletBinding()]
param(
    [string]$DbPath = "runtime-data/axiom.sqlite",
    [int]$GracefulTimeoutSeconds = 120,
    [string]$LockPath = "",
    [switch]$Force,
    [switch]$Isolated
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

$expectedProfile = if ($Isolated) { "isolated" } else { "" }
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

function Test-NodeCommand([string]$CommandLine, [string]$ExpectedDb, [string]$ExpectedProfile = "") {
    if (-not $CommandLine) { return $false }
    $tokens = @(Get-CommandTokens $CommandLine)
    if ($tokens.Count -eq 0) { return $false }
    $executable = $tokens[0].Replace("\", "/").Split("/")[-1].ToLowerInvariant()
    if ($executable -match "^pythonw(?:\d+(?:\.\d+)?)?(?:\.exe)?$") { return $false }
    if ($executable -in @("axiom", "axiom.exe")) {
        $commandIndex = 1
    } elseif ($executable -in @("py", "py.exe") -or $executable -match "^python(?:\d+(?:\.\d+)?)?(?:\.exe)?$") {
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
        if (-not [StringComparer]::OrdinalIgnoreCase.Equals([System.IO.Path]::GetFullPath($actual), $ExpectedDb)) {
            return $false
        }
    } catch {
        return $false
    }
    if (-not [string]::IsNullOrWhiteSpace($ExpectedProfile)) {
        $isolatedFlag = $tokens | Where-Object { $_.ToLowerInvariant() -eq "--isolated" }
        if ($ExpectedProfile -eq "isolated") { return $null -ne $isolatedFlag }
        if ($ExpectedProfile -eq "production") { return $null -eq $isolatedFlag }
    }
    return $true
}
function Test-RecordedStartTime([datetime]$ProcessStartTime, [datetime]$RecordedStartTime) {
    if (
        $ProcessStartTime.Ticks -eq [datetime]::MinValue.Ticks -or
        $RecordedStartTime.Ticks -eq [datetime]::MinValue.Ticks
    ) { return $false }
    return $ProcessStartTime.ToUniversalTime().Ticks -eq $RecordedStartTime.ToUniversalTime().Ticks
}

function Test-ProcessIdentity($Process, [datetime]$StartTime, [string]$ExpectedDb, [string]$ExpectedProfile = "") {
    if (-not $Process) { return $false }
    try {
        $Process.Refresh()
        $currentStart = ([datetime]$Process.StartTime).ToUniversalTime()
        if ($Process.HasExited -or -not (Test-RecordedStartTime $currentStart $StartTime)) { return $false }
    } catch {
        return $false
    }
    return Test-NodeCommand (Get-NodeCommandLine $Process.Id) $ExpectedDb $ExpectedProfile
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
        $raw = [long]$lines[1]
        if ($raw -gt 1000000000000000000) {
            return [DateTimeOffset]::FromUnixTimeMilliseconds([long]($raw / 1000000)).UtcDateTime
        }
        return [datetime]::new($raw, [DateTimeKind]::Utc)
    } catch {
        return [datetime]::MinValue
    }
}


$nodePid = Get-FilePid $pidPath
$recordedStart = Get-FileStartTime $pidPath

# Prefer the lock owner over the PID-file candidate.  A Windows venv
# redirector can remain the PID-file process while its base interpreter owns
# the lock and does the work.
$candidatePid = $nodePid
$candidateQueryFailed = $false
$candidateProcess = if ($candidatePid -gt 0) {
    Get-ProcessSafe $candidatePid ([ref]$candidateQueryFailed)
} else {
    $null
}
if ($candidateQueryFailed) {
    throw "Cannot establish identity for PID $candidatePid; refusing to stop it."
}
if ([string]::IsNullOrWhiteSpace($customLockPath) -and $candidateProcess) {
    $tokens = @(Get-CommandTokens (Get-NodeCommandLine $candidatePid))
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

$lockPid = Get-FilePid $lockPath
$lockQueryFailed = $false
$lockProcess = if ($lockPid -gt 0) {
    Get-ProcessSafe $lockPid ([ref]$lockQueryFailed)
} else {
    $null
}
if ($lockQueryFailed) {
    throw "Cannot establish ownership of Axiom node lock $lockPath; refusing cleanup."
}

$ownerPid = if ($lockPid -gt 0) { $lockPid } else { $candidatePid }
$process = $lockProcess
$processStartTime = [datetime]::MinValue
$candidateStartTime = [datetime]::MinValue
if ($candidateProcess) {
    try { $candidateStartTime = ([datetime]$candidateProcess.StartTime).ToUniversalTime() } catch {
        throw "Cannot establish process identity for PID $candidatePid; refusing to stop it."
    }
}
if ($process) {
    try { $processStartTime = ([datetime]$process.StartTime).ToUniversalTime() } catch {
        throw "Cannot establish process identity for PID $ownerPid; refusing to stop it."
    }
}

if ($lockPid -gt 0 -and $lockPid -ne $candidatePid) {
    if ($candidatePid -le 0 -or $recordedStart.Ticks -eq [datetime]::MinValue.Ticks) {
        throw "Lock $lockPath has owner PID $lockPid but PID file $pidPath cannot prove its launcher identity; refusing to stop."
    }
    if ($candidateProcess -and (
        -not (Test-RecordedStartTime $candidateStartTime $recordedStart) -or
        -not (Test-ProcessIdentity $candidateProcess $recordedStart $dbAbsolute $expectedProfile)
    )) {
        throw "PID $candidatePid changed identity; refusing to stop its child owner."
    }
    if ($lockProcess) {
        try {
            $ownerRecord = Get-CimInstance Win32_Process -Filter "ProcessId=$lockPid" -ErrorAction Stop
            if (-not $ownerRecord -or [int]$ownerRecord.ParentProcessId -ne $candidatePid) {
                throw "Lock owner PID $lockPid is not the verified child of PID $candidatePid; refusing to stop it."
            }
        } catch {
            if ($_.Exception.Message -like "Lock owner PID*") { throw }
            throw "Cannot establish parent identity for lock owner PID $lockPid; refusing to stop it."
        }
        if (-not (Test-NodeCommand (Get-NodeCommandLine $lockPid) $dbAbsolute $expectedProfile)) {
            throw "Lock owner PID $lockPid does not identify this Axiom node; refusing to stop it."
        }
        if ($candidateProcess -and $processStartTime -lt $candidateStartTime) {
            throw "Lock owner PID $lockPid predates its verified launcher PID $candidatePid; refusing to stop it."
        }
    }
}

if ($ownerPid -le 0) {
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

if ($process) {
    if ($recordedStart.Ticks -eq [datetime]::MinValue.Ticks) {
        throw "PID file $pidPath has no persisted process start time; refusing to stop PID $ownerPid."
    }
    if ($lockPid -eq $candidatePid -and (
        -not (Test-RecordedStartTime $processStartTime $recordedStart) -or
        -not (Test-ProcessIdentity $process $recordedStart $dbAbsolute $expectedProfile)
    )) {
        throw "PID $ownerPid changed identity; refusing to stop it."
    }
    if ($lockPid -ne $candidatePid -and -not (Test-ProcessIdentity $process $processStartTime $dbAbsolute $expectedProfile)) {
        throw "Lock owner PID $ownerPid changed identity; refusing to stop it."
    }
    $lockMarker = ""
    try { $lockMarker = (Get-Content -LiteralPath $lockPath -Raw).Trim() } catch {}
    if ([string]::IsNullOrWhiteSpace($lockMarker) -or (Get-FilePid $lockPath) -ne $ownerPid) {
        throw "Axiom node lock $lockPath changed before stop request; refusing to stop PID $ownerPid."
    }
    Set-Content -LiteralPath $stopPath -Value $lockMarker -NoNewline
    $deadline = (Get-Date).AddSeconds($GracefulTimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if (-not (Test-ProcessIdentity $process $processStartTime $dbAbsolute $expectedProfile)) { break }
        Start-Sleep -Milliseconds 250
    }
    try { $process.Refresh() } catch {}
    $stillRunning = $false
    try { $stillRunning = -not $process.HasExited } catch {}
    if ($stillRunning) {
        if (-not $Force) {
            throw "Axiom node PID $ownerPid did not stop within $GracefulTimeoutSeconds seconds; it remains marked for graceful shutdown. Rerun with -Force only if required."
        }
        if (-not (Test-ProcessIdentity $process $processStartTime $dbAbsolute $expectedProfile)) {
            throw "PID $ownerPid changed identity; refusing forced stop."
        }
        Stop-Process -InputObject $process -Force -ErrorAction Stop
        Start-Sleep -Milliseconds 250
        try { $process.Refresh() } catch {}
        try {
            if (-not $process.HasExited) { throw "Axiom node PID $ownerPid did not exit after forced shutdown." }
        } catch {
            throw "Axiom node PID $ownerPid did not exit after forced shutdown."
        }
    }
    Write-Output "Axiom node stopped (PID $ownerPid)."
} else {
    Write-Output "Axiom node process $ownerPid was already stopped."
}

# Do not delete a marker if either the verified owner or its launcher PID was
# reused while shutdown was in progress.
$markerPids = @($ownerPid)
if ($candidatePid -gt 0 -and $candidatePid -ne $ownerPid) { $markerPids += $candidatePid }
foreach ($markerPid in $markerPids) {
    $processQueryFailed = $false
    $currentProcess = Get-ProcessSafe $markerPid ([ref]$processQueryFailed)
    if ($processQueryFailed) {
        throw "Cannot revalidate PID $markerPid; refusing stale-file cleanup."
    }
    if ($currentProcess) {
        if ($markerPid -eq $ownerPid) {
            throw "PID $markerPid was reused; refusing stale-file cleanup."
        }
        if ($candidatePid -eq $markerPid -and $recordedStart.Ticks -ne [datetime]::MinValue.Ticks) {
            if (-not (Test-ProcessIdentity $currentProcess $recordedStart $dbAbsolute $expectedProfile)) {
                throw "Launcher PID $markerPid was reused; refusing stale-file cleanup."
            }
        } else {
            throw "PID $markerPid was reused; refusing stale-file cleanup."
        }
    }
}
if (Test-Path -LiteralPath $pidPath) {
    $currentPidMarker = Get-FilePid $pidPath
    if ($currentPidMarker -eq $candidatePid -or $currentPidMarker -eq $ownerPid) {
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
    if ((Get-FilePid $lockPath) -eq $ownerPid) {
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
    if ($markerPid -le 0 -or $markerPid -eq $ownerPid -or $markerPid -eq $candidatePid) {
        $staleStopPath = "$stopPath.stale.$([guid]::NewGuid().ToString('N'))"
        try {
            Move-Item -LiteralPath $stopPath -Destination $staleStopPath -ErrorAction Stop
            Remove-Item -LiteralPath $staleStopPath -Force -ErrorAction Stop
        } catch {
            throw "Stop marker $stopPath changed during cleanup; refusing cleanup."
        }
    }
}
