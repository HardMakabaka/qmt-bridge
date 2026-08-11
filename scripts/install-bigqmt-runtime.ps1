[CmdletBinding(SupportsShouldProcess)]
param(
    [Parameter(Mandatory = $true)]
    [string]$QmtRoot,
    [Parameter(Mandatory = $true)]
    [string]$AccountId,
    [string]$SourceRoot = "",
    [string]$ZmqEndpoint = "tcp://127.0.0.1:15560",
    [string]$EventZmqEndpoint = "tcp://127.0.0.1:15561",
    [bool]$OrderMethodsEnabled = $true
)

$ErrorActionPreference = "Stop"
$ExpectedSha = "40f7275b15843bd167b7ad424a51d3d547be88df"
$resolvedQmtRoot = [IO.Path]::GetFullPath($QmtRoot)
$pythonRoot = Join-Path $resolvedQmtRoot "python"
$pythonDll = Join-Path $resolvedQmtRoot "bin.x64\python36.dll"

if (-not (Test-Path -LiteralPath $pythonRoot -PathType Container)) {
    throw "QMT python directory was not found: $pythonRoot"
}
if (-not (Test-Path -LiteralPath $pythonDll -PathType Leaf)) {
    throw "QMT embedded Python runtime was not found: $pythonDll"
}
if ([string]::IsNullOrWhiteSpace($AccountId)) {
    throw "AccountId is required"
}
if ($ZmqEndpoint -notmatch '^tcp://(127\.0\.0\.1|localhost|\[::1\]):[0-9]+$') {
    throw "ZmqEndpoint must use TCP loopback"
}
if ($EventZmqEndpoint -notmatch '^tcp://(127\.0\.0\.1|localhost|\[::1\]):[0-9]+$') {
    throw "EventZmqEndpoint must use TCP loopback"
}
if ($EventZmqEndpoint -eq $ZmqEndpoint) {
    throw "EventZmqEndpoint must differ from ZmqEndpoint"
}

$temporarySource = $null
try {
    if ([string]::IsNullOrWhiteSpace($SourceRoot)) {
        $SourceRoot = Split-Path -Parent $PSScriptRoot
    }

    $resolvedSourceRoot = [IO.Path]::GetFullPath($SourceRoot)
    $provenancePath = Join-Path $resolvedSourceRoot "third_party\xtquant_big_convert.UPSTREAM.json"
    if (Test-Path -LiteralPath $provenancePath -PathType Leaf) {
        $provenance = Get-Content -LiteralPath $provenancePath -Raw | ConvertFrom-Json
        if ([string]$provenance.upstream_sha -ne $ExpectedSha) {
            throw "Vendored Big QMT manifest has the wrong upstream SHA"
        }
        foreach ($property in $provenance.file_sha256.PSObject.Properties) {
            $vendoredPath = Join-Path $resolvedSourceRoot ($property.Name.Replace('/', '\'))
            if (-not (Test-Path -LiteralPath $vendoredPath -PathType Leaf)) {
                throw "Vendored Big QMT file is missing: $vendoredPath"
            }
            $actualHash = (Get-FileHash -LiteralPath $vendoredPath -Algorithm SHA256).Hash.ToLowerInvariant()
            if ($actualHash -ne ([string]$property.Value).ToLowerInvariant()) {
                throw "Vendored Big QMT checksum mismatch: $vendoredPath"
            }
        }
    } else {
        $actualSha = (git -C $resolvedSourceRoot rev-parse HEAD).Trim()
        if ($LASTEXITCODE -ne 0 -or $actualSha -ne $ExpectedSha) {
            throw "xtquant_big_convert must be pinned at $ExpectedSha; actual=$actualSha"
        }
    }

    $sourcePackage = Join-Path $resolvedSourceRoot "src\bigqmt_signal_trader"
    $sourceStrategy = Join-Path $resolvedSourceRoot "src\bigqmt_signal_trader_strategy.py"
    $sourceRuntime = Join-Path $resolvedSourceRoot "src\bigqmt_signal_trader_redis_rpc_runtime.py"
    $sourceEntry = Join-Path $resolvedSourceRoot "src\BIGQMT_REDIS_DRYRUN.py"
    $sourceRunner = Join-Path $resolvedSourceRoot "src\MECOSTOCK_BIGQMT_ZMQ.py"
    $sourceModeOverlay = Join-Path $resolvedSourceRoot "src\mecostock_bigqmt_mode_overlay.py"
    foreach ($requiredPath in @($sourcePackage, $sourceStrategy, $sourceRuntime, $sourceEntry, $sourceRunner, $sourceModeOverlay)) {
        if (-not (Test-Path -LiteralPath $requiredPath)) {
            throw "Pinned runtime source is incomplete: $requiredPath"
        }
    }

    $targetPackage = Join-Path $pythonRoot "bigqmt_signal_trader"
    $targetStrategy = Join-Path $pythonRoot "bigqmt_signal_trader_strategy.py"
    $targetRuntime = Join-Path $pythonRoot "bigqmt_signal_trader_redis_rpc_runtime.py"
    $targetEntry = Join-Path $pythonRoot "MECOSTOCK_BIGQMT_BRIDGE.py"
    $targetRunnerSource = Join-Path $pythonRoot "MECOSTOCK_BIGQMT_ZMQ.source.py"
    $targetCompiledModel = Join-Path $pythonRoot "MECOSTOCK_BIGQMT_ZMQ.py"
    $targetConfig = Join-Path $pythonRoot "bigqmt_signal_trader_local_config.py"
    $targetManifest = Join-Path $pythonRoot "MECOSTOCK_BIGQMT_BRIDGE.manifest.json"
    $catalogPath = Join-Path $resolvedQmtRoot "config\indexUserConfig.xml"
    $resolvedTargetPackage = [IO.Path]::GetFullPath($targetPackage)
    $pythonPrefix = [IO.Path]::GetFullPath($pythonRoot).TrimEnd('\') + '\'
    if (-not $resolvedTargetPackage.StartsWith($pythonPrefix, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Big QMT package target escaped the QMT python directory"
    }

    $existingTargets = @(
        $targetPackage,
        $targetStrategy,
        $targetRuntime,
        $targetEntry,
        $targetRunnerSource,
        $targetCompiledModel,
        $targetConfig,
        $targetManifest,
        $catalogPath
    ) | Where-Object { Test-Path -LiteralPath $_ }

    if ($existingTargets.Count -gt 0) {
        $backupRoot = Join-Path $resolvedQmtRoot ("bigqmt_runtime_backups\" + (Get-Date -Format "yyyyMMdd-HHmmss"))
        if ($PSCmdlet.ShouldProcess($backupRoot, "Back up existing Big QMT runtime")) {
            New-Item -ItemType Directory -Path $backupRoot -Force | Out-Null
            foreach ($target in $existingTargets) {
                Copy-Item -LiteralPath $target -Destination $backupRoot -Recurse -Force
            }
        }
    }

    if ($PSCmdlet.ShouldProcess($pythonRoot, "Install pinned Big QMT ZMQ runtime")) {
        if (Test-Path -LiteralPath $targetPackage) {
            Remove-Item -LiteralPath $targetPackage -Recurse -Force
        }
        Copy-Item -LiteralPath $sourcePackage -Destination $targetPackage -Recurse -Force
        Copy-Item -LiteralPath $sourceStrategy -Destination $targetStrategy -Force
        Copy-Item -LiteralPath $sourceRuntime -Destination $targetRuntime -Force
        Copy-Item -LiteralPath $sourceEntry -Destination $targetEntry -Force
        [IO.File]::AppendAllText(
            $targetEntry,
            ("`r`n`r`n" + [IO.File]::ReadAllText($sourceModeOverlay)),
            [Text.UTF8Encoding]::new($false)
        )
        Copy-Item -LiteralPath $sourceRunner -Destination $targetRunnerSource -Force

        $escapedAccountId = $AccountId.Replace("'", "\'")
        $escapedEndpoint = $ZmqEndpoint.Replace("'", "\'")
        $escapedEventEndpoint = $EventZmqEndpoint.Replace("'", "\'")
        $pythonOrderMethodsEnabled = if ($OrderMethodsEnabled) { "True" } else { "False" }
        $config = @"
BIGQMT_ACCOUNT_ID = '$escapedAccountId'

BIGQMT_REDIS_CONFIG = {
    'transport': 'zmq',
    'zmq': {
        'bind_address': '$escapedEndpoint',
        'redis_discovery_enabled': False,
    },
    'rpc_allow_order_methods': $pythonOrderMethodsEnabled,
    'rpc_process_in_listener': True,
    'rpc_listener_methods': ('*',),
    'rpc_background_threads': True,
    'schedule_adjust': True,
    'schedule_adjust_interval': '200nMilliSecond',
    'full_tick_cache_enabled': False,
    'download_jobs_enabled': False,
    'exec_events_enabled': True,
    'exec_events_transport': 'zmq',
    'exec_events_zmq': {
        'bind_address': '$escapedEventEndpoint',
        'maxlen': 2000,
    },
}
"@
        [IO.File]::WriteAllText($targetConfig, $config, [Text.UTF8Encoding]::new($false))

        $manifest = [ordered]@{
            schema_version = "mecostock_bigqmt_runtime_v1"
            upstream_repository = "https://github.com/litaolemo/xtquant_big_convert"
            upstream_sha = $ExpectedSha
            installed_at = (Get-Date).ToString("o")
            transport = "zmq"
            endpoint = $ZmqEndpoint
            event_endpoint = $EventZmqEndpoint
            order_methods_enabled = $OrderMethodsEnabled
            terminal_mode_attestation = $true
            terminal_mode_attestation_source = "qmt_request_id_and_terminal_log"
            terminal_mode_overlay_sha256 = (Get-FileHash -LiteralPath $sourceModeOverlay -Algorithm SHA256).Hash.ToLowerInvariant()
            entry = [IO.Path]::GetFileName($targetEntry)
            runner_source = [IO.Path]::GetFileName($targetRunnerSource)
            compiled_model = [IO.Path]::GetFileName($targetCompiledModel)
        }
        [IO.File]::WriteAllText(
            $targetManifest,
            ($manifest | ConvertTo-Json -Depth 4),
            [Text.UTF8Encoding]::new($false)
        )
    }

    if (-not (Test-Path -LiteralPath $catalogPath -PathType Leaf)) {
        throw "QMT strategy catalog was not found: $catalogPath"
    }
    $compiledModelReady = Test-Path -LiteralPath $targetCompiledModel -PathType Leaf
    if ($compiledModelReady -and $PSCmdlet.ShouldProcess($catalogPath, "Register Big QMT bridge in embedded Python mode")) {
        $catalogText = [IO.File]::ReadAllText($catalogPath)
        $staleEntryPattern = '\s*<catalog\s+[^>]*name="MECOSTOCK_BIGQMT_BRIDGE"[^>]*/>'
        $catalogText = [Regex]::Replace($catalogText, $staleEntryPattern, "", 1)
        $entryPattern = '<catalog\s+[^>]*name="MECOSTOCK_BIGQMT_ZMQ"[^>]*/>'
        $catalogEntry = '            <catalog scriptType="1" formulaCatalogModelType="4" systemProvidedStrategy="0" strategymall="0" name="MECOSTOCK_BIGQMT_ZMQ" type="2" simpleRun="0"/>'
        if ([Regex]::IsMatch($catalogText, $entryPattern)) {
            $catalogText = [Regex]::Replace($catalogText, $entryPattern, $catalogEntry, 1)
        } else {
            $parentPattern = '(<catalog\s+scriptType="1"\s+formulaCatalogModelType="4"\s+systemProvidedStrategy="1"\s+strategymall="0"\s+name="我的策略"\s+type="1"\s+simpleRun="0">)'
            if (-not [Regex]::IsMatch($catalogText, $parentPattern)) {
                throw "QMT strategy catalog does not contain the expected 我的策略 section"
            }
            $newline = if ($catalogText.Contains("`r`n")) { "`r`n" } else { "`n" }
            $catalogText = [Regex]::Replace(
                $catalogText,
                $parentPattern,
                ('$1' + $newline + $catalogEntry),
                1
            )
        }
        [IO.File]::WriteAllText($catalogPath, $catalogText, [Text.UTF8Encoding]::new($false))
    }

    Write-Output ([ordered]@{
        qmt_root = $resolvedQmtRoot
        python_root = $pythonRoot
        entry = $targetEntry
        runner_source = $targetRunnerSource
        compiled_model = $targetCompiledModel
        compiled_model_ready = $compiledModelReady
        config = $targetConfig
        upstream_sha = $ExpectedSha
        transport = "zmq"
        endpoint = $ZmqEndpoint
        order_methods_enabled = $OrderMethodsEnabled
        terminal_mode_attestation = $true
        terminal_mode_attestation_source = "qmt_request_id_and_terminal_log"
        catalog = $catalogPath
        embedded_python_mode = $compiledModelReady
        terminal_start_autorun_managed_by_qmt = $true
    } | ConvertTo-Json -Depth 3)
} finally {
    if ($null -ne $temporarySource -and (Test-Path -LiteralPath $temporarySource)) {
        $resolvedTemporarySource = [IO.Path]::GetFullPath($temporarySource)
        $temporaryPrefix = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd('\') + '\'
        if (-not $resolvedTemporarySource.StartsWith($temporaryPrefix, [StringComparison]::OrdinalIgnoreCase)) {
            throw "Temporary source escaped the Windows temporary directory"
        }
        Remove-Item -LiteralPath $resolvedTemporarySource -Recurse -Force
    }
}
