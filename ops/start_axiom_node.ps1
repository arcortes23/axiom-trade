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
    [string]$LockPath = "",
    [string]$LogPath = "",
    [string]$Python = "python"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$dbInput = if ([System.IO.Path]::IsPathRooted($DbPath)) { $DbPath } else { Join-Path $root $DbPath }
$dbAbsolute = [System.IO.Path]::GetFullPath($dbInput)
$pidPath = "$dbAbsolute.node.pid"
$logInput = if ([string]::IsNullOrWhiteSpace($LogPath)) { "$dbAbsolute.log" } elseif ([System.IO.Path]::IsPathRooted($LogPath)) { $LogPath } else { Join-Path $root $LogPath }
$lockInput = if ([string]::IsNullOrWhiteSpace($LockPath)) { "$dbAbsolute.lock" } elseif ([System.IO.Path]::IsPathRooted($LockPath)) { $LockPath } else { Join-Path $root $LockPath }
$logPath = [System.IO.Path]::GetFullPath($logInput)
$lockPath = [System.IO.Path]::GetFullPath($lockInput)
$stopPath = "$dbAbsolute.stop"
[System.IO.Directory]::CreateDirectory((Split-Path -Parent $dbAbsolute)) | Out-Null
[System.IO.Directory]::CreateDirectory((Split-Path -Parent $logPath)) | Out-Null
[System.IO.Directory]::CreateDirectory((Split-Path -Parent $lockPath)) | Out-Null
$pythonExecutable = $Python
if (-not [System.IO.Path]::IsPathRooted($Python) -and ($Python.Contains("\") -or $Python.Contains("/") -or $Python.StartsWith("."))) {
    $pythonExecutable = [System.IO.Path]::GetFullPath((Join-Path $root $Python))
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
        if (Test-NodeCommand (Get-NodeCommandLine $lockPid) $dbAbsolute) {
            throw "Axiom node already holds $lockPath (PID $lockPid)."
        }
        throw "Axiom node lock $lockPath belongs to another live PID $lockPid; refusing to remove it."
    }
    $currentLockPid = Get-FilePid $lockPath
    if ($currentLockPid -eq $lockPid) {
        $processQueryFailed = $false
        $recheckProcess = Get-ProcessSafe $lockPid ([ref]$processQueryFailed)
        if ($processQueryFailed) {
            throw "Cannot revalidate Axiom node lock $lockPath; refusing to start."
        }
        if ($recheckProcess) {
            throw "Axiom node lock $lockPath was reused by live PID $lockPid; refusing to remove it."
        }
        $staleLockPath = "$lockPath.stale.$([guid]::NewGuid().ToString('N'))"
        try {
            Move-Item -LiteralPath $lockPath -Destination $staleLockPath -ErrorAction Stop
            Remove-Item -LiteralPath $staleLockPath -Force -ErrorAction Stop
        } catch {
            throw "Axiom node lock $lockPath changed while checking its stale owner; refusing to start."
        }
    }
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
        if (Test-NodeCommand (Get-NodeCommandLine $existingPid) $dbAbsolute) {
            $existingStart = Get-FileStartTime $pidPath
            if ($existingStart.Ticks -eq [datetime]::MinValue.Ticks) {
                throw "PID file $pidPath has no persisted process start time; refusing to trust PID $existingPid."
            }
            if (-not (Test-ProcessIdentity $existing $existingStart $dbAbsolute)) {
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

function Quote-ProcessArgument([string]$Value) {
    return '"' + $Value.Replace('"', '\"') + '"'
}
$arguments = @(
    "-m", "axiom.cli", "node-run",
    "--db", (Quote-ProcessArgument $dbAbsolute),
    "--interval", $IntervalSeconds.ToString([Globalization.CultureInfo]::InvariantCulture),
    "--depth", $Depth,
    "--max-markets", $MaxMarkets,
    "--crypto-source", $CryptoSource,
    "--log", (Quote-ProcessArgument $logPath),
    "--lock", (Quote-ProcessArgument $lockPath)
)
$process = Start-Process -FilePath $pythonExecutable -ArgumentList $arguments -WorkingDirectory $root -WindowStyle Hidden -PassThru
$processStartTime = [datetime]$process.StartTime
$ready = $false
$probe = 'import sqlite3,sys; c=sqlite3.connect(sys.argv[1], timeout=1); r=c.execute(\"SELECT status FROM worker_state WHERE worker_name=?\", (\"axiom-node\",)).fetchone(); print(r[0] if r else \"\"); c.close()'
for ($attempt = 0; $attempt -lt 40; $attempt++) {
    Start-Sleep -Milliseconds 250
    if ($process.HasExited) { break }
    $ownerPid = 0
    if (-not (Test-Path -LiteralPath $lockPath)) { continue }
    try { $ownerPid = Get-FilePid $lockPath } catch { continue }
    if ($ownerPid -ne $process.Id) { continue }
    if (-not (Test-ProcessIdentity $process ([datetime]$process.StartTime) $dbAbsolute)) { continue }
    $state = ((& $pythonExecutable -c $probe $dbAbsolute 2>$null) | Out-String).Trim()
    if ($LASTEXITCODE -eq 0 -and $state -eq "running") {
        $ready = $true
        break
    }
}
if (-not $ready) {
    if (-not $process.HasExited) { Stop-Process -InputObject $process -Force -ErrorAction SilentlyContinue }
    throw "Axiom node failed readiness; no running worker state at $dbAbsolute."
}
$persistedStart = ([datetime]$processStartTime).ToUniversalTime()
Set-Content -LiteralPath $pidPath -Value @([string]$process.Id, [string]$persistedStart.Ticks)
Write-Output "Axiom node started (PID $($process.Id)); lock $lockPath; log $logPath."
