[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$QmtRoot,
    [Parameter(Mandatory = $true)]
    [string]$AccountId,
    [string]$SourceRoot = "",
    [string]$ZmqEndpoint = "tcp://127.0.0.1:15560",
    [string]$EventZmqEndpoint = "tcp://127.0.0.1:15561",
    [bool]$OrderMethodsEnabled = $true,
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

# This wrapper intentionally has no installer logic. The Python entry point is
# the single implementation used by source checkouts and installed wheels.
if ([string]::IsNullOrWhiteSpace($SourceRoot)) {
    $SourceRoot = Split-Path -Parent $PSScriptRoot
}
$resolvedSourceRoot = [IO.Path]::GetFullPath($SourceRoot)
$python = Join-Path $resolvedSourceRoot ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    $python = Join-Path $resolvedSourceRoot ".venv/bin/python"
}
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    $python = (Get-Command python -ErrorAction Stop).Source
}

$previousPythonPath = $env:PYTHONPATH
try {
    # This wrapper imports its own checkout's installer. SourceRoot identifies
    # the independently checksum-pinned embedded asset tree to deploy.
    $sourcePath = Join-Path (Split-Path -Parent $PSScriptRoot) "src"
    $env:PYTHONPATH = if ([string]::IsNullOrWhiteSpace($previousPythonPath)) {
        $sourcePath
    } else {
        $sourcePath + [IO.Path]::PathSeparator + $previousPythonPath
    }
    $arguments = @(
        "-m", "qmt_bridge.install_runtime",
        "--qmt-root", $QmtRoot,
        "--account-id", $AccountId,
        "--source-root", $resolvedSourceRoot,
        "--zmq-endpoint", $ZmqEndpoint,
        "--event-zmq-endpoint", $EventZmqEndpoint
    )
    $arguments += if ($OrderMethodsEnabled) { "--order-methods-enabled" } else { "--no-order-methods-enabled" }
    if ($WhatIf) { $arguments += "--what-if" }
    & $python @arguments
    if ($LASTEXITCODE -ne 0) { throw "qmt-install-runtime failed with exit code $LASTEXITCODE" }
} finally {
    $env:PYTHONPATH = $previousPythonPath
}
