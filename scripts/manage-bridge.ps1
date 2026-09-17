[CmdletBinding()]
param(
    [Parameter(Position = 0, Mandatory = $true)]
    [ValidateSet("start", "stop", "status")]
    [string]$Action,
    [switch]$Foreground,
    [switch]$Force,
    [string]$StatePath = "",
    [string]$LogDirectory = "",
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ServerArgs
)

$ErrorActionPreference = "Stop"
$projectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$python = Join-Path $projectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw "Project virtualenv interpreter was not found: $python"
}
$python = [IO.Path]::GetFullPath($python)
$statePath = if ([string]::IsNullOrWhiteSpace($StatePath)) {
    Join-Path $projectRoot "qmt-bridge.process.json"
} else {
    [IO.Path]::GetFullPath($StatePath)
}
$logDirectory = if ([string]::IsNullOrWhiteSpace($LogDirectory)) {
    Join-Path $projectRoot "logs"
} else {
    [IO.Path]::GetFullPath($LogDirectory)
}
$stdoutPath = Join-Path $logDirectory "qmt-bridge.stdout.log"
$stderrPath = Join-Path $logDirectory "qmt-bridge.stderr.log"
$moduleArguments = @("-m", "qmt_bridge.server.cli") + $ServerArgs
$traceEnabled = [string]::IsNullOrWhiteSpace($env:QMT_BRIDGE_TRACE_ENABLED) -or $env:QMT_BRIDGE_TRACE_ENABLED -match '^(1|true|yes|on)$'
$controllerInstanceId = [guid]::NewGuid().ToString("N")
$traceDirectory = if ([string]::IsNullOrWhiteSpace($env:QMT_BRIDGE_TRACE_DIR)) {
    if (-not [string]::IsNullOrWhiteSpace($env:LOCALAPPDATA)) {
        Join-Path $env:LOCALAPPDATA "QmtBridge\traces"
    } elseif (-not [string]::IsNullOrWhiteSpace($LogDirectory)) {
        Join-Path $logDirectory "telemetry"
    } else {
        Join-Path $projectRoot "logs\telemetry"
    }
} else {
    [IO.Path]::GetFullPath($env:QMT_BRIDGE_TRACE_DIR)
}

function Write-LifecycleTelemetry([string]$EventName, [hashtable]$Fields = @{}) {
    if (-not $traceEnabled) { return }
    try {
        New-Item -ItemType Directory -Path $traceDirectory -Force | Out-Null
        $path = Join-Path $traceDirectory ("bridge_controller-{0}-{1}.jsonl" -f $PID, $controllerInstanceId.Substring(0, 12))
        if ((Test-Path -LiteralPath $path) -and (Get-Item -LiteralPath $path).Length -ge 1048576) {
            for ($i = 2; $i -ge 1; $i--) {
                $older = "$path.$i"
                $newer = if ($i -eq 1) { $path } else { "$path.$($i - 1)" }
                if (Test-Path -LiteralPath $newer) { Move-Item -LiteralPath $newer -Destination $older -Force }
            }
        }
        $entry = [ordered]@{
            schema_version = 1
            timestamp_utc = [datetime]::UtcNow.ToString("o")
            process_instance_id = $controllerInstanceId
            service_role = "bridge_controller"
            event_name = $EventName
            trace_id = [string]$Fields.trace_id
            outcome = [string]$Fields.outcome
            critical = [bool]$Fields.critical
        }
        foreach ($key in $Fields.Keys) {
            if ($key -notin @("shutdown_token", "nonce", "token")) { $entry[$key] = $Fields[$key] }
        }
        Add-Content -LiteralPath $path -Value ($entry | ConvertTo-Json -Compress) -Encoding utf8
    } catch {
        # Observability must never change lifecycle behavior.
    }
}

function Get-ManagedState {
    if (-not (Test-Path -LiteralPath $statePath -PathType Leaf)) { return $null }
    try { return Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json }
    catch { throw "Bridge state file is invalid: $statePath" }
}

function Get-ProcessStartUtc([int]$ProcessId) {
    return (Get-Process -Id $ProcessId -ErrorAction Stop).StartTime.ToUniversalTime().ToString("o")
}

function Test-ManagedProcess($State) {
    try {
        $process = Get-CimInstance Win32_Process -Filter "ProcessId = $([int]$State.pid)" -ErrorAction Stop
        if ($null -eq $process) { return $false }
        $recordedStart = ([datetime]$State.start_time_utc).ToUniversalTime().ToString("o")
        if ((Get-ProcessStartUtc ([int]$State.pid)) -ne $recordedStart) { throw "PID $($State.pid) was reused" }
        $storedExe = [IO.Path]::GetFullPath([string]$State.executable)
        if (-not $storedExe.Equals($python, [StringComparison]::OrdinalIgnoreCase)) {
            throw "PID $($State.pid) belongs to a different project virtualenv"
        }
        $actualExe = [IO.Path]::GetFullPath([string]$process.ExecutablePath)
        if (-not $actualExe.Equals($python, [StringComparison]::OrdinalIgnoreCase)) {
            throw "PID $($State.pid) executable does not match bridge state"
        }
        if ([string]$process.CommandLine -notmatch [regex]::Escape("-m qmt_bridge.server.cli")) {
            throw "PID $($State.pid) command line is not the managed bridge"
        }
        Write-LifecycleTelemetry "process.identity_verified" @{
            trace_id = [string]$State.trace_id; outcome = "success"; pid = [int]$State.pid
            created_at_utc = $recordedStart
        }
        return $true
    } catch [System.Management.Automation.RuntimeException] { throw }
    catch { return $false }
}

function Remove-StaleState($State) {
    if ($null -ne $State -and -not (Test-ManagedProcess $State)) {
        Remove-Item -LiteralPath $statePath -Force
        return $null
    }
    return $State
}

switch ($Action) {
    "status" {
        $state = Get-ManagedState
        if ($null -eq $state) { Write-Output "stopped"; exit 3 }
        if (Test-ManagedProcess $state) {
            Write-Output ("running pid={0} started={1}" -f $state.pid, $state.start_time_utc)
            exit 0
        }
        Write-Output "stale state"; exit 3
    }
    "start" {
        $state = Remove-StaleState (Get-ManagedState)
        if ($null -ne $state) { throw "Bridge is already running with PID $($state.pid)" }
        if ($Foreground) {
            Set-Location -LiteralPath $projectRoot
            if ([string]::IsNullOrWhiteSpace($env:QMT_BRIDGE_TRACE_ID)) { $env:QMT_BRIDGE_TRACE_ID = [guid]::NewGuid().ToString("N") }
            Write-LifecycleTelemetry "process.foreground_start" @{ trace_id = $env:QMT_BRIDGE_TRACE_ID; outcome = "requested"; critical = $true }
            & $python @moduleArguments
            exit $LASTEXITCODE
        }
        New-Item -ItemType Directory -Path $logDirectory -Force | Out-Null
        $shutdownFile = $statePath + ".shutdown"
        $shutdownToken = [guid]::NewGuid().ToString("N")
        $traceId = if ([string]::IsNullOrWhiteSpace($env:QMT_BRIDGE_TRACE_ID)) { [guid]::NewGuid().ToString("N") } else { $env:QMT_BRIDGE_TRACE_ID }
        Remove-Item -LiteralPath $shutdownFile -Force -ErrorAction SilentlyContinue
        $moduleArguments += @("--shutdown-file", $shutdownFile, "--shutdown-token", $shutdownToken)
        $argumentLine = ($moduleArguments | ForEach-Object {
            if ($_ -match '[\s"]') { '"' + $_.Replace('"', '\"') + '"' } else { $_ }
        }) -join ' '
        $previousTraceId = $env:QMT_BRIDGE_TRACE_ID
        $previousTraceDirectory = $env:QMT_BRIDGE_TRACE_DIR
        try {
            $env:QMT_BRIDGE_TRACE_ID = $traceId
            $env:QMT_BRIDGE_TRACE_DIR = $traceDirectory
            $child = Start-Process -FilePath $python -ArgumentList $argumentLine -WorkingDirectory $projectRoot `
                -WindowStyle Hidden -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath -PassThru
        } finally {
            $env:QMT_BRIDGE_TRACE_ID = $previousTraceId
            $env:QMT_BRIDGE_TRACE_DIR = $previousTraceDirectory
        }
        Start-Sleep -Milliseconds 300
        if ($child.HasExited) { throw "Bridge exited during startup; inspect $stderrPath" }
        New-Item -ItemType Directory -Path (Split-Path -Parent $statePath) -Force | Out-Null
        [ordered]@{
            schema_version = 1; pid = $child.Id; start_time_utc = Get-ProcessStartUtc $child.Id
            executable = $python; command = "-m qmt_bridge.server.cli"
            shutdown_file = $shutdownFile; shutdown_token = $shutdownToken
            trace_id = $traceId
        } | ConvertTo-Json | Set-Content -LiteralPath $statePath -Encoding utf8 -NoNewline
        Write-LifecycleTelemetry "process.started" @{
            trace_id = $traceId; outcome = "success"; pid = $child.Id
            created_at_utc = Get-ProcessStartUtc $child.Id; state_path = $statePath
        }
        Write-Output ("started pid={0}" -f $child.Id)
    }
    "stop" {
        $state = Get-ManagedState
        if ($null -eq $state) { Write-Output "stopped"; exit 0 }
        if (-not (Test-ManagedProcess $state)) { throw "Refusing to stop PID $($state.pid): bridge identity cannot be verified" }
        $shutdownFile = [string]$state.shutdown_file
        $shutdownToken = [string]$state.shutdown_token
        if ([string]::IsNullOrWhiteSpace($shutdownFile) -or [string]::IsNullOrWhiteSpace($shutdownToken)) {
            throw "Refusing to stop PID $($state.pid): graceful shutdown identity is missing"
        }
        $shutdownDirectory = Split-Path -Parent $shutdownFile
        New-Item -ItemType Directory -Path $shutdownDirectory -Force | Out-Null
        Write-LifecycleTelemetry "process.stop_requested" @{
            trace_id = [string]$state.trace_id; outcome = "requested"; pid = [int]$state.pid
            created_at_utc = ([datetime]$state.start_time_utc).ToUniversalTime().ToString("o")
        }
        [IO.File]::WriteAllText($shutdownFile, $shutdownToken, [Text.UTF8Encoding]::new($false))
        for ($i = 0; $i -lt 40; $i++) {
            Start-Sleep -Milliseconds 250
            if (-not (Get-Process -Id ([int]$state.pid) -ErrorAction SilentlyContinue)) {
                Remove-Item -LiteralPath $statePath -Force
                Remove-Item -LiteralPath $shutdownFile -Force -ErrorAction SilentlyContinue
                Write-LifecycleTelemetry "process.stopped" @{ trace_id = [string]$state.trace_id; outcome = "graceful"; pid = [int]$state.pid }
                Write-Output "stopped gracefully"; exit 0
            }
        }
        if (-not $Force) {
            throw "Bridge did not exit after a graceful request; rerun with -Force after checking $stderrPath"
        }
        if (-not (Test-ManagedProcess $state)) { throw "Refusing forced stop: bridge identity changed" }
        Write-LifecycleTelemetry "process.force_requested" @{ trace_id = [string]$state.trace_id; outcome = "requested"; pid = [int]$state.pid }
        Stop-Process -Id ([int]$state.pid) -Force
        for ($i = 0; $i -lt 20; $i++) {
            Start-Sleep -Milliseconds 250
            if (-not (Get-Process -Id ([int]$state.pid) -ErrorAction SilentlyContinue)) {
                Remove-Item -LiteralPath $statePath -Force
                Remove-Item -LiteralPath $shutdownFile -Force -ErrorAction SilentlyContinue
                Write-LifecycleTelemetry "process.stopped" @{ trace_id = [string]$state.trace_id; outcome = "forced"; pid = [int]$state.pid }
                Write-Output "stopped forcibly"; exit 0
            }
        }
        throw "Bridge remained alive after forced stop; state retained for inspection"
    }
}
