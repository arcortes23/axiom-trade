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
    [switch]$Isolated,
    [string]$Python = "python"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$dbInput = if ([System.IO.Path]::IsPathRooted($DbPath)) { $DbPath } else { Join-Path $root $DbPath }
$dbAbsolute = [System.IO.Path]::GetFullPath($dbInput)
$pidPath = "$dbAbsolute.node.pid"
$logInput = if ([string]::IsNullOrWhiteSpace($LogPath)) { "$dbAbsolute.log" } elseif ([System.IO.Path]::IsPathRooted($LogPath)) { $LogPath } else { Join-Path $root $LogPath }
$lockPath = "$dbAbsolute.lock"
$logPath = [System.IO.Path]::GetFullPath($logInput)
$stopPath = "$dbAbsolute.stop"
[System.IO.Directory]::CreateDirectory((Split-Path -Parent $pidPath)) | Out-Null
[System.IO.Directory]::CreateDirectory((Split-Path -Parent $dbAbsolute)) | Out-Null
[System.IO.Directory]::CreateDirectory((Split-Path -Parent $logPath)) | Out-Null
[System.IO.Directory]::CreateDirectory((Split-Path -Parent $lockPath)) | Out-Null
$pythonExecutable = $Python
if (-not [System.IO.Path]::IsPathRooted($Python) -and ($Python.Contains("\") -or $Python.Contains("/") -or $Python.StartsWith("."))) {
    $pythonExecutable = [System.IO.Path]::GetFullPath((Join-Path $root $Python))
}

function Get-ConfiguredExecutionProfile {
    $rawProfile = [Environment]::GetEnvironmentVariable("AXIOM_EXECUTION_PROFILE", "Process")
    if ([string]::IsNullOrEmpty($rawProfile)) { return "production" }
    if ($rawProfile -eq "production" -or $rawProfile -eq "isolated") { return $rawProfile }
    throw "AXIOM_EXECUTION_PROFILE must be exactly 'production' or 'isolated'; refusing to start."
}

$ambientProfile = Get-ConfiguredExecutionProfile
$expectedProfile = if ($Isolated) { "isolated" } else { $ambientProfile }
$effectiveIsolated = $expectedProfile -eq "isolated"
if ([System.IO.Path]::GetFileNameWithoutExtension($pythonExecutable).ToLowerInvariant() -match "^pythonw(?:\d+(?:\.\d+)?)?$") {
    throw "Python launcher '$Python' cannot be used for node readiness because pythonw has no stdout."
}

function Quote-ProcessArgument([AllowNull()][string]$Value) {
    # Start-Process joins ArgumentList into one command line.  Quote using
    # CommandLineToArgvW-compatible escaping, including trailing backslashes.
    if ($null -eq $Value) { return '""' }
    $text = [string]$Value
    if ($text.Length -eq 0) { return '""' }
    $escaped = $text -replace '(\\*)"', '$1$1\"'
    $escaped = $escaped -replace '(\\+)$', '$1$1'
    return '"' + $escaped + '"'
}


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
function Test-ExactStartTime([datetime]$ExpectedStartTime, [datetime]$ActualStartTime) {
    if (
        $ExpectedStartTime.Ticks -eq [datetime]::MinValue.Ticks -or
        $ActualStartTime.Ticks -eq [datetime]::MinValue.Ticks
    ) { return $false }
    return $ExpectedStartTime.ToUniversalTime().Ticks -eq $ActualStartTime.ToUniversalTime().Ticks
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


if (Test-Path -LiteralPath $lockPath) {
    $lockPid = Get-FilePid $lockPath
    if ($lockPid -le 0) {
        throw "Axiom node lock $lockPath exists but has no valid owner; refusing to remove it."
    }
    $processQueryFailed = $false
    $lockProcess = Get-ProcessSafe $lockPid ([ref]$processQueryFailed)
    if ($processQueryFailed) {
        throw "Cannot establish ownership of Axiom node lock $lockPath; refusing to start."
    }
    if ($lockProcess) {
        if (Test-NodeCommand (Get-NodeCommandLine $lockPid) $dbAbsolute $expectedProfile) {
            throw "Axiom node already holds $lockPath (PID $lockPid)."
        }
        throw "Axiom node lock $lockPath belongs to another live PID $lockPid; refusing to remove it."
    }
    throw "NODE_STALE_LOCK_MANUAL_RECOVERY: Axiom node lock $lockPath has stale owner PID $lockPid; manual recovery is required; refusing to start."
}

if (Test-Path -LiteralPath $pidPath) {
    $existingPid = Get-FilePid $pidPath
    $existing = $null
    if ($existingPid -gt 0) {
        $processQueryFailed = $false
        $existing = Get-ProcessSafe $existingPid ([ref]$processQueryFailed)
        if ($processQueryFailed) {
            throw "Cannot establish identity for PID file $pidPath; refusing to start."
        }
    }
    if ($existing) {
        if (Test-NodeCommand (Get-NodeCommandLine $existingPid) $dbAbsolute $expectedProfile) {
            $existingStart = Get-FileStartTime $pidPath
            if ($existingStart.Ticks -eq [datetime]::MinValue.Ticks) {
                throw "PID file $pidPath has no persisted process start time; refusing to trust PID $existingPid."
            }
            if (-not (Test-ProcessIdentity $existing $existingStart $dbAbsolute $expectedProfile)) {
                throw "PID $existingPid changed identity; refusing to trust PID file $pidPath."
            }
            if ((Get-FilePid $lockPath) -ne $existingPid) {
                throw "PID $existingPid matches this database but does not own lock $lockPath; refusing to start a duplicate node."
            }
            Write-Output "Axiom node already running (PID $existingPid)."
            return
        }
        throw "PID file $pidPath belongs to another live process; refusing to remove it."
    }
    $stalePidPath = "$pidPath.stale.$([guid]::NewGuid().ToString('N'))"
    try {
        Move-Item -LiteralPath $pidPath -Destination $stalePidPath -ErrorAction Stop
        Remove-Item -LiteralPath $stalePidPath -Force -ErrorAction Stop
    } catch {
        throw "PID file $pidPath changed while checking its stale owner; refusing to start."
    }
}

# A marker left by a crashed or forcibly stopped owner is stale once no owner remains.
if (Test-Path -LiteralPath $stopPath) {
    $staleStopPath = "$stopPath.stale.$([guid]::NewGuid().ToString('N'))"
    try {
        Move-Item -LiteralPath $stopPath -Destination $staleStopPath -ErrorAction Stop
        Remove-Item -LiteralPath $staleStopPath -Force -ErrorAction Stop
    } catch {
        throw "Stop marker $stopPath changed while checking its stale owner; refusing to start."
    }
}

$arguments = @(
    "-m", "axiom.cli", "node-run",
    "--db", (Quote-ProcessArgument $dbAbsolute),
    "--interval", $IntervalSeconds.ToString([Globalization.CultureInfo]::InvariantCulture),
    "--depth", $Depth,
    "--max-markets", $MaxMarkets,
    "--crypto-source", $CryptoSource,
    "--log", (Quote-ProcessArgument $logPath),
    "--lock", (Quote-ProcessArgument $lockPath),
    "--pid", (Quote-ProcessArgument $pidPath),
    "--cycles", "0"
)
if ($effectiveIsolated) { $arguments += "--isolated" }

$launcherProcess = $null
$process = $null
$launcherStartTime = [datetime]::MinValue
$processStartTime = [datetime]::MinValue
$ownerPid = 0
$status = $null
$ready = $false
$startedSuccessfully = $false
$previousExecutionProfile = [Environment]::GetEnvironmentVariable("AXIOM_EXECUTION_PROFILE", "Process")
try {
    # Set the child environment explicitly for both launch and probe.  Restore
    # the caller's environment before returning, even when readiness fails.
    [Environment]::SetEnvironmentVariable("AXIOM_EXECUTION_PROFILE", $expectedProfile, "Process")
    $launcherProcess = Start-Process -FilePath $pythonExecutable -ArgumentList $arguments -WorkingDirectory $root -WindowStyle Hidden -PassThru
    $process = $launcherProcess
    $launcherStartTime = ([datetime]$launcherProcess.StartTime).ToUniversalTime()
    $processStartTime = $launcherStartTime

    for ($attempt = 0; $attempt -lt 40; $attempt++) {
        Start-Sleep -Milliseconds 250
        # A py/venv launcher may exit after handing ownership to its child.
        # Continue probing the lock owner instead of mistaking that handoff
        # for a failed launch.
        if (-not (Test-Path -LiteralPath $lockPath)) { continue }
        $ownerPid = Get-FilePid $lockPath
        if ($ownerPid -le 0) { continue }
        $ownerQueryFailed = $false
        $ownerProcess = Get-ProcessSafe $ownerPid ([ref]$ownerQueryFailed)
        if ($ownerQueryFailed -or -not $ownerProcess) { continue }

        if ($ownerPid -eq $launcherProcess.Id) {
            $process = $launcherProcess
            $processStartTime = $launcherStartTime
        } else {
            try {
                $ownerRecord = Get-CimInstance Win32_Process -Filter "ProcessId=$ownerPid" -ErrorAction Stop
                if (-not $ownerRecord -or [int]$ownerRecord.ParentProcessId -ne $launcherProcess.Id) { continue }
                if (-not (Test-NodeCommand ([string]$ownerRecord.CommandLine) $dbAbsolute $expectedProfile)) { continue }
                $ownerStartTime = ([datetime]$ownerProcess.StartTime).ToUniversalTime()
                if ($ownerStartTime -lt $launcherStartTime) { continue }
            } catch {
                continue
            }
            $process = $ownerProcess
            $processStartTime = $ownerStartTime
        }

        # The lock is created before the PID marker.  Require both markers to
        # identify the same owner before trusting readiness output.
        if ((Get-FilePid $lockPath) -ne $ownerPid -or (Get-FilePid $pidPath) -ne $ownerPid) { continue }
        if (-not (Test-ProcessIdentity $process $processStartTime $dbAbsolute $expectedProfile)) { continue }

        # Use only PowerShell/.NET for the independent identity/profile probe.
        # Windows PowerShell 5.1 can otherwise strip an embedded Python -c
        # program while constructing native arguments.
        $probePid = Get-FilePid $pidPath
        if ($probePid -ne $ownerPid) { continue }
        $probeQueryFailed = $false
        $probeProcess = Get-ProcessSafe $probePid ([ref]$probeQueryFailed)
        if ($probeQueryFailed -or -not $probeProcess) { continue }
        if (-not (Test-ProcessIdentity $probeProcess $processStartTime $dbAbsolute $expectedProfile)) { continue }
        if (-not (Test-NodeCommand (Get-NodeCommandLine $probePid) $dbAbsolute $expectedProfile)) { continue }

        $statusArguments = @(
            "-m", "axiom.cli", "node-status",
            "--db", $dbAbsolute,
            "--lock", $lockPath,
            "--log", $logPath,
            "--pid", $pidPath
        )
        if ($effectiveIsolated) { $statusArguments += "--isolated" }
        $statusText = ((& $pythonExecutable @statusArguments 2>$null) | Out-String).Trim()
        if ($LASTEXITCODE -ne 0 -or -not $statusText) { continue }
        try { $status = $statusText | ConvertFrom-Json } catch { continue }
        if (
            [string]$status.status -eq "running" -and
            [int]$status.pid -eq $ownerPid -and
            [int]$status.lock_owner_pid -eq $ownerPid -and
            [bool]$status.worker_alive -and
            [bool]$status.worker_identity_valid -and
            [string]$status.execution_profile -eq $expectedProfile -and
            [string]$status.worker.execution_profile -eq $expectedProfile
        ) {
            $ready = $true
            break
        }
    }
    if (-not $ready) {
        throw "Axiom node failed readiness; no running worker state at $dbAbsolute."
    }

    # Revalidate the lock owner and both marker PIDs after the independent
    # probe and status query.  The lock marker's OS timestamp is written by
    # the node itself and is a second representation of the owner's start
    # time; comparing it again with Process.StartTime can reject a valid
    # venv launcher/base-child handoff due to timestamp precision/encoding.
    $finalLockPid = Get-FilePid $lockPath
    $finalPidMarker = Get-FilePid $pidPath
    if (
        $finalLockPid -ne $ownerPid -or
        $finalPidMarker -ne $ownerPid -or
        $process.Id -ne $ownerPid
    ) {
        throw "Axiom node ownership changed during readiness; refusing startup."
    }
    $finalQueryFailed = $false
    $finalProcess = Get-ProcessSafe $ownerPid ([ref]$finalQueryFailed)
    if ($finalQueryFailed -or -not $finalProcess) {
        throw "Axiom node owner disappeared during readiness; refusing startup."
    }
    if (-not (Test-ProcessIdentity $finalProcess $processStartTime $dbAbsolute $expectedProfile)) {
        throw "Axiom node owner identity changed during readiness; refusing startup."
    }
    try {
        $finalOwnerStartTime = ([datetime]$finalProcess.StartTime).ToUniversalTime()
    } catch {
        throw "Cannot revalidate Axiom node owner start time during readiness; refusing startup."
    }
    if (-not (Test-ExactStartTime $processStartTime $finalOwnerStartTime)) {
        throw "Axiom node owner identity changed during readiness; refusing startup."
    }
    if ($ownerPid -ne $launcherProcess.Id) {
        try {
            $finalOwnerRecord = Get-CimInstance Win32_Process -Filter "ProcessId=$ownerPid" -ErrorAction Stop
            if (
                -not $finalOwnerRecord -or
                [int]$finalOwnerRecord.ParentProcessId -ne $launcherProcess.Id -or
                -not (Test-NodeCommand ([string]$finalOwnerRecord.CommandLine) $dbAbsolute $expectedProfile)
            ) {
                throw "Axiom node owner parent or command changed during readiness; refusing startup."
            }
        } catch {
            if ($_.Exception.Message -like "Axiom node owner parent*") { throw }
            throw "Cannot revalidate Axiom node owner parent during readiness; refusing startup."
        }
    }
    $process = $finalProcess
    $processStartTime = $finalOwnerStartTime

    $startedSuccessfully = $true
    Write-Output "Axiom node started (PID $($process.Id)); lock $lockPath; log $logPath."
} catch {
    if (-not $startedSuccessfully) {
        if ($process -and $processStartTime.Ticks -ne [datetime]::MinValue.Ticks -and (Test-ProcessIdentity $process $processStartTime $dbAbsolute $expectedProfile)) {
            Stop-Process -InputObject $process -Force -ErrorAction SilentlyContinue
        }
        if ($launcherProcess -and $launcherProcess.Id -ne $ownerPid -and $launcherStartTime.Ticks -ne [datetime]::MinValue.Ticks -and (Test-ProcessIdentity $launcherProcess $launcherStartTime $dbAbsolute $expectedProfile)) {
            Stop-Process -InputObject $launcherProcess -Force -ErrorAction SilentlyContinue
        }
    }
    throw
} finally {
    [Environment]::SetEnvironmentVariable("AXIOM_EXECUTION_PROFILE", $previousExecutionProfile, "Process")
}
