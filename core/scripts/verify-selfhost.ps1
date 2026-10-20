[CmdletBinding()]
param(
    [switch]$PrepareOnly,
    [switch]$BridgeFixture,
    [string]$TargetRepository,
    [string]$PlanningRepository,
    [string]$TaskFile,
    [string]$WorkDirectory,
    [string]$Role,
    [string]$Model,
    [int]$CallTimeoutSeconds,
    [int]$BuildTimeoutSeconds = 180,
    [string]$RuntimeRepository,
    [string]$ExecutorRepository,
    [string]$Docker,
    [string]$FixtureCLI
)

$ErrorActionPreference = 'Stop'
if ($BridgeFixture) { $script:bridgeWatch = [Diagnostics.Stopwatch]::StartNew() }

if ($PrepareOnly -and $BridgeFixture) {
    [Console]::Error.WriteLine('bridge_fixture_switch_conflict')
    exit 2
}
if (-not $PrepareOnly -and -not $BridgeFixture) {
    [Console]::Error.WriteLine('prepare_only_required')
    exit 2
}

$script:phase = 'path'
$script:failureCode = 'path_invalid'
$script:workRoot = $null
$script:runRoot = $null
$script:privateDirectory = $null
$script:runID = $null
$script:resultPath = $null
$script:ownedContainers = [Collections.Generic.List[string]]::new()
$script:bridgeStart = $null
$script:bridgeDeadline = $null
$script:bridgePhaseDeadline = $null
$script:bridgeOverallMilliseconds = 0.0
$script:bridgePreflightMilliseconds = 0.0
$script:bridgeBuildMilliseconds = 0.0
$script:bridgeSnapshotPrepared = $false
$script:result = [ordered]@{
    schema = 'roster-selfhost-result/1'
    mode = 'prepare'
    status = 'failed'
    error = $null
    started_at = [DateTime]::UtcNow.ToString('o')
    completed_at = $null
    models = [ordered]@{
        requested = [ordered]@{ planner = $null; implementer = $null; reviewer = $null }
        actual = [ordered]@{ planner = $null; implementer = $null; reviewer = $null }
    }
    usage = [ordered]@{ planner = $null; implementer = $null; reviewer = $null }
    actual_model_calls = $false
    task_id = $null
    correlation_id = $null
    roles = [ordered]@{}
    review_verdict = $null
    verification = @()
    changed_files = @()
    patch_path = $null
    result_path = $null
    work_project = $null
    planning_project = $null
    planning_patch_path = $null
    subscriptions = @()
    cleanup = [ordered]@{ complete = $false; errors = @() }
}

function Get-FullPath([string]$Path) {
    if ([string]::IsNullOrWhiteSpace($Path)) { throw 'path' }
    try { return [IO.Path]::GetFullPath($Path) } catch { throw 'path' }
}

function Get-ComparablePath([string]$Path) {
    $full = Get-FullPath $Path
    $root = [IO.Path]::GetPathRoot($full)
    $trimmed = $full.TrimEnd([char[]]@([IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar))
    if ($trimmed.Length -lt $root.Length) { return $root }
    return $trimmed
}

function Get-PathStringComparer {
    if ([IO.Path]::DirectorySeparatorChar -eq '\') { return [StringComparer]::OrdinalIgnoreCase }
    return [StringComparer]::Ordinal
}

function Get-PathStringComparison {
    if ([IO.Path]::DirectorySeparatorChar -eq '\') { return [StringComparison]::OrdinalIgnoreCase }
    return [StringComparison]::Ordinal
}

function Test-PathWithinRoot([string]$Path, [string]$Root) {
    $candidate = Get-ComparablePath $Path
    $rootPath = Get-ComparablePath $Root
    $comparison = Get-PathStringComparison
    if ($candidate.Equals($rootPath, $comparison)) { return $false }
    $separator = [IO.Path]::DirectorySeparatorChar
    $prefix = $rootPath
    if (-not $prefix.EndsWith([string]$separator, [StringComparison]::Ordinal)) { $prefix += $separator }
    return $candidate.StartsWith($prefix, $comparison)
}

function Assert-ChildPath([string]$Path, [string]$Root) {
    if (-not (Test-PathWithinRoot $Path $Root)) { throw 'path' }
}

function Test-PathOverlap([string]$Left, [string]$Right) {
    $leftPath = Get-ComparablePath $Left
    $rightPath = Get-ComparablePath $Right
    $comparison = Get-PathStringComparison
    return $leftPath.Equals($rightPath, $comparison) -or
        (Test-PathWithinRoot $leftPath $rightPath) -or
        (Test-PathWithinRoot $rightPath $leftPath)
}

function Assert-NoReparsePath([string]$Path, [switch]$AllowMissing) {
    $full = Get-FullPath $Path
    $root = [IO.Path]::GetPathRoot($full)
    $current = $root
    if (Test-Path -LiteralPath $current) {
        $rootItem = Get-Item -LiteralPath $current -Force
        if (($rootItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0 -or $rootItem.LinkType) { throw 'path' }
    }
    $remaining = $full.Substring($root.Length)
    foreach ($segment in @($remaining -split '[\\/]') | Where-Object { $_ }) {
        $current = Join-Path $current $segment
        if (-not (Test-Path -LiteralPath $current)) {
            if ($AllowMissing) { return }
            throw 'path'
        }
        $item = Get-Item -LiteralPath $current -Force
        if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0 -or $item.LinkType) { throw 'path' }
    }
}

function Wait-BridgeProcess([Diagnostics.Process]$Process, [double]$DeadlineMilliseconds) {
    if ($DeadlineMilliseconds -gt 0) {
        $remaining = Get-BridgeRemainingMilliseconds $DeadlineMilliseconds
        if ($remaining -le 0 -or -not $Process.WaitForExit($remaining)) {
            try { $Process.Kill($true) } catch { }
            [void]$Process.WaitForExit(1000)
            throw 'deadline'
        }
    } else { $Process.WaitForExit() }
}

function Get-GitRoot([string]$Path, [double]$DeadlineMilliseconds = 0) {
    $startInfo = [Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = 'git'; $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true; $startInfo.RedirectStandardError = $true
    $startInfo.StandardOutputEncoding = [Text.UTF8Encoding]::new($false, $true)
    $startInfo.StandardErrorEncoding = [Text.UTF8Encoding]::new($false, $true)
    foreach ($argument in @('-C', $Path, 'rev-parse', '--show-toplevel')) { [void]$startInfo.ArgumentList.Add($argument) }
    $process = [Diagnostics.Process]::new(); $process.StartInfo = $startInfo
    try {
        if (-not $process.Start()) { throw 'path' }
        $outputTask = $process.StandardOutput.ReadToEndAsync(); $errorTask = $process.StandardError.ReadToEndAsync()
        Wait-BridgeProcess $process $DeadlineMilliseconds
        $output = $outputTask.GetAwaiter().GetResult(); [void]$errorTask.GetAwaiter().GetResult()
        if ($process.ExitCode -ne 0 -or -not $output.Trim()) { throw 'path' }
        return Get-ComparablePath $output.Trim()
    } catch { if ($_.Exception.Message -eq 'deadline') { throw 'deadline' }; throw 'path' }
    finally { $process.Dispose() }
}

function Get-TrackedFiles([string]$Repository, [double]$DeadlineMilliseconds = 0) {
    $startInfo = [Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = 'git'
    $startInfo.WorkingDirectory = $Repository
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $startInfo.StandardOutputEncoding = [Text.UTF8Encoding]::new($false, $true)
    $startInfo.StandardErrorEncoding = [Text.UTF8Encoding]::new($false, $true)
    foreach ($argument in @('-c', 'core.quotepath=false', 'ls-files', '--cached', '-z')) { [void]$startInfo.ArgumentList.Add($argument) }
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    try {
        if (-not $process.Start()) { throw 'path' }
        $errorTask = $process.StandardError.ReadToEndAsync()
        $outputTask = $process.StandardOutput.ReadToEndAsync()
        Wait-BridgeProcess $process $DeadlineMilliseconds
        $raw = $outputTask.GetAwaiter().GetResult()
        [void]$errorTask.GetAwaiter().GetResult()
        if ($process.ExitCode -ne 0) { throw 'path' }
    } catch {
        if ($_.Exception.Message -eq 'deadline') { throw 'deadline' }
        throw 'path'
    } finally {
        $process.Dispose()
    }
    $files = [Collections.Generic.List[string]]::new()
    $seen = [Collections.Generic.HashSet[string]]::new((Get-PathStringComparer))
    foreach ($entry in $raw.Split([char]0, [StringSplitOptions]::RemoveEmptyEntries)) {
        if ($DeadlineMilliseconds -gt 0 -and (Get-BridgeRemainingMilliseconds $DeadlineMilliseconds) -le 0) { throw 'deadline' }
        $relative = [string]$entry
        if (-not $relative) { throw 'path' }
        $segments = $relative.Split('/')
        if ($relative.StartsWith('/') -or $relative -match '^[A-Za-z]:' -or @($segments | Where-Object { $_ -in @('', '.', '..') }).Count -gt 0) { throw 'path' }
        if (-not $seen.Add($relative)) { throw 'path' }
        $files.Add($relative)
    }
    $files.Sort((Get-PathStringComparer))
    return $files.ToArray()
}

function Normalize-TaskPath([object]$Value) {
    if ($Value -isnot [string] -or [string]::IsNullOrWhiteSpace([string]$Value)) { throw 'task' }
    $relative = ([string]$Value).Replace('\', '/')
    if ($relative.StartsWith('/') -or $relative -match '^[A-Za-z]:' -or $relative.Contains(':')) { throw 'task' }
    $segments = $relative.Split('/')
    foreach ($segment in $segments) {
        if (-not $segment -or $segment -in @('.', '..') -or $segment -match '[<>:"|?*\x00]' -or $segment.EndsWith('.') -or $segment.EndsWith(' ')) { throw 'task' }
        if ($segment -match '^(?i:con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\..*)?$') { throw 'task' }
    }
    return $segments -join '/'
}

function Assert-TrackedFile([string]$Repository, [string[]]$Tracked, [string]$Relative, [double]$DeadlineMilliseconds = 0) {
    $normalized = Normalize-TaskPath $Relative
    $trackedSet = [Collections.Generic.HashSet[string]]::new((Get-PathStringComparer))
    foreach ($path in $Tracked) {
        if ($DeadlineMilliseconds -gt 0 -and (Get-BridgeRemainingMilliseconds $DeadlineMilliseconds) -le 0) { throw 'deadline' }
        [void]$trackedSet.Add($path)
    }
    if (-not $trackedSet.Contains($normalized)) { throw 'task' }
    $full = Join-Path $Repository ($normalized.Replace('/', [IO.Path]::DirectorySeparatorChar))
    Assert-ChildPath $full $Repository
    Assert-NoReparsePath $full
    if (-not (Test-Path -LiteralPath $full -PathType Leaf)) { throw 'task' }
    return $normalized
}

function Copy-PreflightFile([string]$Source, [string]$Destination) {
    $input = $null
    $output = $null
    try {
        $input = [IO.File]::Open($Source, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::Read)
        $output = [IO.File]::Open($Destination, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
        $buffer = [byte[]]::new(1048576)
        while ($true) {
            if ((Get-BridgeRemainingMilliseconds $script:bridgePreflightMilliseconds) -le 0) { throw 'deadline' }
            $read = $input.Read($buffer, 0, $buffer.Length)
            if ($read -le 0) { break }
            $output.Write($buffer, 0, $read)
        }
    } finally {
        if ($null -ne $output) { $output.Dispose() }
        if ($null -ne $input) { $input.Dispose() }
    }
}

function Copy-GitSnapshot([string]$Source, [string]$Destination, [string[]]$Files, [string]$RunRoot) {
    Assert-ChildPath $Destination $RunRoot
    New-Item -ItemType Directory -Path $Destination | Out-Null
    Assert-NoReparsePath $Destination
    foreach ($relative in $Files) {
        if ($script:bridgePreflightMilliseconds -gt 0 -and (Get-BridgeRemainingMilliseconds $script:bridgePreflightMilliseconds) -le 0) { throw 'deadline' }
        $sourceFile = Join-Path $Source ($relative.Replace('/', [IO.Path]::DirectorySeparatorChar))
        Assert-ChildPath $sourceFile $Source
        Assert-NoReparsePath $sourceFile -AllowMissing
        if (-not (Test-Path -LiteralPath $sourceFile)) { continue }
        if (-not (Test-Path -LiteralPath $sourceFile -PathType Leaf)) { throw 'path' }
        $destinationFile = Join-Path $Destination ($relative.Replace('/', [IO.Path]::DirectorySeparatorChar))
        Assert-ChildPath $destinationFile $Destination
        $parent = Split-Path -Parent $destinationFile
        if (-not (Test-Path -LiteralPath $parent)) { New-Item -ItemType Directory -Path $parent | Out-Null }
        Assert-NoReparsePath $parent
        if ($script:bridgePreflightMilliseconds -gt 0) { Copy-PreflightFile $sourceFile $destinationFile }
        else { Copy-Item -LiteralPath $sourceFile -Destination $destinationFile }
    }
}

function Remove-OwnedPrivateDirectory([double]$DeadlineMilliseconds) {
    if ([string]::IsNullOrWhiteSpace($script:privateDirectory)) { return }
    $privateItem = Get-Item -LiteralPath $script:privateDirectory -Force -ErrorAction SilentlyContinue
    if ($null -eq $privateItem) { return }
    if ([string]::IsNullOrWhiteSpace($script:workRoot) -or [string]::IsNullOrWhiteSpace($script:runID)) { throw 'cleanup' }
    $expected = Join-Path $script:workRoot ".roster-selfhost-private-$script:runID"
    $candidate = Get-ComparablePath $script:privateDirectory
    if (-not $candidate.Equals((Get-ComparablePath $expected), [StringComparison]::OrdinalIgnoreCase)) { throw 'cleanup' }
    Assert-ChildPath $candidate $script:workRoot
    Assert-NoReparsePath $candidate
    $pending = [Collections.Generic.Stack[object]]::new()
    $pending.Push(@{ path = $candidate; visited = $false })
    while ($pending.Count -gt 0) {
        if ($DeadlineMilliseconds -gt 0 -and $script:bridgeWatch -and (Get-BridgeRemainingMilliseconds $DeadlineMilliseconds) -le 0) { throw 'cleanup_deadline' }
        $node = $pending.Pop()
        $directory = [string]$node.path
        if ($node.visited) {
            Remove-Item -LiteralPath $directory -Force
            continue
        }
        $pending.Push(@{ path = $directory; visited = $true })
        foreach ($entry in [IO.Directory]::EnumerateFileSystemEntries($directory)) {
            if ($DeadlineMilliseconds -gt 0 -and $script:bridgeWatch -and (Get-BridgeRemainingMilliseconds $DeadlineMilliseconds) -le 0) { throw 'cleanup_deadline' }
            $item = Get-Item -LiteralPath $entry -Force
            if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0 -or $item.LinkType) { throw 'cleanup' }
            if ($item.PSIsContainer) { $pending.Push(@{ path = $item.FullName; visited = $false }) }
            else {
                if ($DeadlineMilliseconds -gt 0 -and $script:bridgeWatch -and (Get-BridgeRemainingMilliseconds $DeadlineMilliseconds) -le 0) { throw 'cleanup_deadline' }
                Remove-Item -LiteralPath $item.FullName -Force
            }
        }
    }
}

function Get-BridgeRepository([string]$Path, [switch]$RequireClean, [string[]]$AllowedChanges = @()) {
    if (-not (Test-Path -LiteralPath $Path -PathType Container)) { throw 'path' }
    $full = Get-ComparablePath $Path
    Assert-NoReparsePath $full
    if (-not (Get-PathStringComparer).Equals((Get-GitRoot $full $script:bridgePreflightMilliseconds), $full)) { throw 'path' }
    if ($RequireClean) {
        $startInfo = [Diagnostics.ProcessStartInfo]::new()
        $startInfo.FileName = 'git'; $startInfo.UseShellExecute = $false
        $startInfo.RedirectStandardOutput = $true; $startInfo.RedirectStandardError = $true
        $startInfo.StandardOutputEncoding = [Text.UTF8Encoding]::new($false, $true)
        foreach ($argument in @('-C', $full, '-c', 'core.quotepath=false', 'status', '--porcelain=v1', '-z', '--untracked-files=all')) { [void]$startInfo.ArgumentList.Add($argument) }
        $process = [Diagnostics.Process]::new(); $process.StartInfo = $startInfo
        try {
            if (-not $process.Start()) { throw 'path' }
            $outputTask = $process.StandardOutput.ReadToEndAsync(); $errorTask = $process.StandardError.ReadToEndAsync()
            Wait-BridgeProcess $process $script:bridgePreflightMilliseconds
            $status = $outputTask.GetAwaiter().GetResult(); [void]$errorTask.GetAwaiter().GetResult()
            if ($process.ExitCode -ne 0) { throw 'path' }
            $allowed = [Collections.Generic.HashSet[string]]::new((Get-PathStringComparer))
            foreach ($change in $AllowedChanges) { [void]$allowed.Add($change) }
            $records = $status.Split([char]0, [StringSplitOptions]::RemoveEmptyEntries)
            foreach ($record in $records) {
                if ($record.Length -lt 4 -or $record[2] -ne ' ') { throw 'path' }
                $state = $record.Substring(0, 2)
                $changedPath = $record.Substring(3)
                if ($state.Contains('R') -or $state.Contains('C') -or -not $allowed.Contains($changedPath)) { throw 'path' }
            }
            if ($AllowedChanges.Count -eq 0 -and $records.Count -gt 0) { throw 'path' }
        } catch { if ($_.Exception.Message -eq 'deadline') { throw 'deadline' }; throw 'path' }
        finally { $process.Dispose() }
    }
    return $full
}

function Get-BridgeRemainingMilliseconds([double]$DeadlineMilliseconds) {
    $remaining = [math]::Min($DeadlineMilliseconds, $script:bridgeOverallMilliseconds) - $script:bridgeWatch.Elapsed.TotalMilliseconds
    return [int][math]::Floor($remaining)
}

function Invoke-DockerBounded([string[]]$Arguments, [double]$DeadlineMilliseconds) {
    $remaining = Get-BridgeRemainingMilliseconds $DeadlineMilliseconds
    if ($remaining -le 0) { throw 'deadline' }
    $startInfo = [Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $script:dockerExecutable
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $startInfo.StandardOutputEncoding = [Text.UTF8Encoding]::new($false, $true)
    $startInfo.StandardErrorEncoding = [Text.UTF8Encoding]::new($false, $true)
    foreach ($argument in $Arguments) { [void]$startInfo.ArgumentList.Add([string]$argument) }
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    try {
        if (-not $process.Start()) { throw 'docker' }
        $stdoutTask = $process.StandardOutput.ReadToEndAsync()
        $stderrTask = $process.StandardError.ReadToEndAsync()
        if (-not $process.WaitForExit($remaining)) {
            try { $process.Kill($true) } catch { }
            [void]$process.WaitForExit(1000)
            throw 'deadline'
        }
        $stdout = $stdoutTask.GetAwaiter().GetResult()
        [void]$stderrTask.GetAwaiter().GetResult()
        if ($process.ExitCode -ne 0) { throw 'docker' }
        return $stdout.Trim()
    } catch {
        if ($_.Exception.Message -in @('deadline', 'docker')) { throw $_.Exception.Message }
        throw 'docker'
    } finally {
        $process.Dispose()
    }
}

function Invoke-GitDirectoryDiff([string]$Left, [string]$Right, [string]$RunRoot, [double]$DeadlineMilliseconds) {
    $startInfo = [Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = 'git'
    $startInfo.WorkingDirectory = $RunRoot
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $startInfo.StandardOutputEncoding = [Text.UTF8Encoding]::new($false, $true)
    $startInfo.StandardErrorEncoding = [Text.UTF8Encoding]::new($false, $true)
    foreach ($argument in @('diff', '--no-index', '--binary', '--no-ext-diff', '--no-color', '--src-prefix=a/', '--dst-prefix=b/', '--', (Split-Path -Leaf $Left), (Split-Path -Leaf $Right))) { [void]$startInfo.ArgumentList.Add($argument) }
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    try {
        if (-not $process.Start()) { throw 'patch' }
        $errorTask = $process.StandardError.ReadToEndAsync()
        $outputTask = $process.StandardOutput.ReadToEndAsync()
        $remaining = Get-BridgeRemainingMilliseconds $DeadlineMilliseconds
        if ($remaining -le 0 -or -not $process.WaitForExit($remaining)) {
            try { $process.Kill($true) } catch { }
            [void]$process.WaitForExit(1000)
            throw 'patch'
        }
        $outputText = $outputTask.GetAwaiter().GetResult()
        [void]$errorTask.GetAwaiter().GetResult()
        if ($process.ExitCode -notin @(0, 1)) { throw 'patch' }
        return $outputText.Replace("a/$(Split-Path -Leaf $Left)/", 'a/').Replace("b/$(Split-Path -Leaf $Right)/", 'b/')
    } catch {
        throw 'patch'
    } finally {
        $process.Dispose()
    }
}

function Get-SnapshotChanges([string]$Baseline, [string]$Project, [double]$DeadlineMilliseconds = 0) {
    $comparer = Get-PathStringComparer
    $before = [Collections.Generic.Dictionary[string, string]]::new($comparer)
    $after = [Collections.Generic.Dictionary[string, string]]::new($comparer)
    foreach ($entry in @(@{ root = $Baseline; map = $before }, @{ root = $Project; map = $after })) {
        $pending = [Collections.Generic.Stack[string]]::new()
        $pending.Push($entry.root)
        while ($pending.Count -gt 0) {
            if ($DeadlineMilliseconds -gt 0 -and (Get-BridgeRemainingMilliseconds $DeadlineMilliseconds) -le 0) { throw 'deadline' }
            $directory = $pending.Pop()
            foreach ($item in Get-ChildItem -LiteralPath $directory -Force) {
                if ($DeadlineMilliseconds -gt 0 -and (Get-BridgeRemainingMilliseconds $DeadlineMilliseconds) -le 0) { throw 'deadline' }
                if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0 -or $item.LinkType) { throw 'path' }
                if ($item.PSIsContainer) { $pending.Push($item.FullName); continue }
                $relative = [IO.Path]::GetRelativePath($entry.root, $item.FullName).Replace('\', '/')
                if ($DeadlineMilliseconds -gt 0) {
                    $hash = [Security.Cryptography.IncrementalHash]::CreateHash([Security.Cryptography.HashAlgorithmName]::SHA256)
                    $stream = $null
                    try {
                        $stream = [IO.File]::OpenRead($item.FullName)
                        $buffer = [byte[]]::new(1048576)
                        while ($true) {
                            if ((Get-BridgeRemainingMilliseconds $DeadlineMilliseconds) -le 0) { throw 'deadline' }
                            $read = $stream.Read($buffer, 0, $buffer.Length)
                            if ($read -le 0) { break }
                            $hash.AppendData($buffer, 0, $read)
                        }
                        $entry.map[$relative] = [Convert]::ToHexString($hash.GetHashAndReset())
                    } finally {
                        if ($null -ne $stream) { $stream.Dispose() }
                        $hash.Dispose()
                    }
                } else { $entry.map[$relative] = (Get-FileHash -LiteralPath $item.FullName -Algorithm SHA256).Hash }
            }
        }
    }
    $paths = [Collections.Generic.HashSet[string]]::new($comparer)
    foreach ($key in $before.Keys) { [void]$paths.Add($key) }
    foreach ($key in $after.Keys) { [void]$paths.Add($key) }
    $changed = [Collections.Generic.List[string]]::new()
    foreach ($path in $paths) {
        if (-not $before.ContainsKey($path) -or -not $after.ContainsKey($path) -or $before[$path] -ne $after[$path]) { $changed.Add($path) }
    }
    $changed.Sort($comparer)
    return $changed.ToArray()
}

function Write-BridgeJson([string]$Path, [object]$Value) {
    [IO.File]::WriteAllText($Path, (ConvertTo-Json -InputObject $Value -Depth 80 -Compress), [Text.UTF8Encoding]::new($false))
}

function Invoke-CoreCommand([string]$Name, [object]$InputValue, [string]$InputPath, [double]$DeadlineMilliseconds) {
    Write-BridgeJson $InputPath $InputValue
    $raw = Invoke-DockerBounded @('exec', $script:coreContainer, '/private/roster', $Name, '--file', ('/private/' + (Split-Path -Leaf $InputPath))) $DeadlineMilliseconds
    try { return ConvertFrom-Json -InputObject $raw -Depth 80 -ErrorAction Stop } catch { throw 'wire' }
}

function Invoke-BridgeFixture {
    if (-not $script:bridgeWatch) { $script:bridgeWatch = [Diagnostics.Stopwatch]::StartNew() }
    $script:phase = 'path'
    $script:failureCode = 'path_invalid'
    if ([string]::IsNullOrWhiteSpace($Role) -or $Role -notin @('planner', 'implementer', 'reviewer') -or
        [string]::IsNullOrWhiteSpace($Model) -or $Model.Length -gt 200 -or $Model -match '[\x00-\x1f\x7f]' -or
        $CallTimeoutSeconds -lt 30 -or $CallTimeoutSeconds -gt 3600 -or
        $BuildTimeoutSeconds -lt 30 -or $BuildTimeoutSeconds -gt 1800 -or
        [string]::IsNullOrWhiteSpace($RuntimeRepository) -or [string]::IsNullOrWhiteSpace($ExecutorRepository) -or
        [string]::IsNullOrWhiteSpace($Docker) -or [string]::IsNullOrWhiteSpace($FixtureCLI)) { throw 'path' }
    $script:bridgeOverallMilliseconds = ($BuildTimeoutSeconds + $CallTimeoutSeconds + 150) * 1000.0
    $script:bridgePreflightMilliseconds = 30000.0
    $script:bridgeBuildStartedMilliseconds = $null
    $script:bridgeBuildDurationMilliseconds = $null
    $script:bridgeStartupStartedMilliseconds = $null
    $script:bridgeStartupDurationMilliseconds = $null

    foreach ($value in @($TargetRepository, $PlanningRepository, $TaskFile, $WorkDirectory)) {
        if ((Get-BridgeRemainingMilliseconds $script:bridgePreflightMilliseconds) -le 0) { throw 'deadline' }
        if ([string]::IsNullOrWhiteSpace($value)) { throw 'path' }
    }
    $targetPath = Get-FullPath $TargetRepository
    $planningPath = Get-FullPath $PlanningRepository
    $taskPath = Get-FullPath $TaskFile
    $script:workRoot = Get-FullPath $WorkDirectory
    $runtimePath = Get-BridgeRepository (Get-FullPath $RuntimeRepository) -RequireClean
    $executorPath = Get-BridgeRepository (Get-FullPath $ExecutorRepository) -RequireClean -AllowedChanges @('scripts/verify-selfhost.ps1', 'examples/selfhost-agents.mjs')
    $expectedExecutor = Get-ComparablePath (Join-Path $PSScriptRoot '..')
    if (-not (Get-PathStringComparer).Equals($executorPath, $expectedExecutor)) { throw 'path' }
    if (-not (Test-Path -LiteralPath (Join-Path $runtimePath 'dist/main.js') -PathType Leaf) -or
        -not (Test-Path -LiteralPath (Join-Path $runtimePath 'node_modules') -PathType Container)) { throw 'path' }

    foreach ($repository in @($targetPath, $planningPath)) {
        if ((Get-BridgeRemainingMilliseconds $script:bridgePreflightMilliseconds) -le 0) { throw 'deadline' }
        if (-not (Test-Path -LiteralPath $repository -PathType Container)) { throw 'path' }
        Assert-NoReparsePath $repository
        if (-not (Get-PathStringComparer).Equals((Get-GitRoot $repository $script:bridgePreflightMilliseconds), (Get-ComparablePath $repository))) { throw 'path' }
    }
    $roots = @($targetPath, $planningPath, $runtimePath, $executorPath)
    for ($i = 0; $i -lt $roots.Count; $i++) {
        if ((Get-BridgeRemainingMilliseconds $script:bridgePreflightMilliseconds) -le 0) { throw 'deadline' }
        for ($j = $i + 1; $j -lt $roots.Count; $j++) { if (Test-PathOverlap $roots[$i] $roots[$j]) { throw 'path' } }
    }
    foreach ($repository in $roots) { if (Test-PathOverlap $script:workRoot $repository) { throw 'path' } }
    if (Test-Path -LiteralPath $script:workRoot -PathType Leaf) { throw 'path' }
    Assert-NoReparsePath $script:workRoot -AllowMissing
    if (-not [IO.Path]::IsPathFullyQualified($FixtureCLI)) { throw 'path' }
    $fixtureTools = Join-Path $script:workRoot 'fixture-tools'
    $fixtureCliPath = Get-FullPath $FixtureCLI
    if (-not (Test-Path -LiteralPath $fixtureTools -PathType Container) -or -not (Test-Path -LiteralPath $fixtureCliPath -PathType Leaf)) { throw 'path' }
    Assert-NoReparsePath $fixtureTools
    Assert-NoReparsePath $fixtureCliPath
    Assert-ChildPath $fixtureCliPath $fixtureTools
    foreach ($repository in $roots) { if (Test-PathOverlap $fixtureCliPath $repository) { throw 'path' } }
    $dockerCommand = Get-Command -Name $Docker -CommandType Application -ErrorAction Stop | Select-Object -First 1
    $script:dockerExecutable = [string]$dockerCommand.Source

    $script:phase = 'task'
    $script:failureCode = 'task_invalid'
    if (-not (Test-Path -LiteralPath $taskPath -PathType Leaf)) { throw 'path' }
    Assert-NoReparsePath $taskPath
    $task = ConvertFrom-Json -InputObject ([IO.File]::ReadAllText($taskPath, [Text.Encoding]::UTF8)) -Depth 60 -ErrorAction Stop
    if ($null -eq $task -or $task -is [array]) { throw 'task' }
    foreach ($key in @('task_id', 'goal', 'requirements', 'planning_files', 'allowed_files', 'required_verification')) { if ($null -eq $task.$key) { throw 'task' } }
    if ([string]$task.task_id -notmatch '^[A-Za-z0-9][A-Za-z0-9_.-]{0,39}$' -or [string]::IsNullOrWhiteSpace([string]$task.goal) -or
        $task.requirements -isnot [array] -or $task.requirements.Count -lt 1 -or $task.planning_files -isnot [array] -or
        $task.allowed_files -isnot [array] -or $task.allowed_files.Count -lt 1 -or $task.required_verification -isnot [array] -or $task.required_verification.Count -lt 1) { throw 'task' }
    foreach ($requirement in $task.requirements) {
        if ((Get-BridgeRemainingMilliseconds $script:bridgePreflightMilliseconds) -le 0) { throw 'deadline' }
        if ($requirement -isnot [string] -or [string]::IsNullOrWhiteSpace($requirement)) { throw 'task' }
    }
    foreach ($check in $task.required_verification) {
        if ((Get-BridgeRemainingMilliseconds $script:bridgePreflightMilliseconds) -le 0) { throw 'deadline' }
        if (-not $check.image -or [string]$check.image -notmatch '^[A-Za-z0-9][A-Za-z0-9._/:@-]{0,200}$' -or $check.command -isnot [array] -or $check.command.Count -lt 1) { throw 'task' }
        foreach ($argument in $check.command) { if ($argument -isnot [string] -or [string]::IsNullOrWhiteSpace($argument)) { throw 'task' } }
    }
    $targetFiles = @(Get-TrackedFiles $targetPath $script:bridgePreflightMilliseconds)
    $planningFiles = @(Get-TrackedFiles $planningPath $script:bridgePreflightMilliseconds)
    $planningAllowed = [Collections.Generic.List[string]]::new()
    $planningSet = [Collections.Generic.HashSet[string]]::new((Get-PathStringComparer))
    foreach ($path in $task.planning_files) {
        if ((Get-BridgeRemainingMilliseconds $script:bridgePreflightMilliseconds) -le 0) { throw 'deadline' }
        $relative = Assert-TrackedFile $planningPath $planningFiles $path $script:bridgePreflightMilliseconds
        if (-not $planningSet.Add($relative)) { throw 'task' }
        $planningAllowed.Add($relative)
    }
    $codeAllowed = [Collections.Generic.List[string]]::new()
    $codeSet = [Collections.Generic.HashSet[string]]::new((Get-PathStringComparer))
    foreach ($path in $task.allowed_files) {
        if ((Get-BridgeRemainingMilliseconds $script:bridgePreflightMilliseconds) -le 0) { throw 'deadline' }
        $relative = Assert-TrackedFile $targetPath $targetFiles $path $script:bridgePreflightMilliseconds
        if (-not $codeSet.Add($relative)) { throw 'task' }
        $codeAllowed.Add($relative)
    }
    $task.planning_files = $planningAllowed.ToArray()
    $task.allowed_files = $codeAllowed.ToArray()

    $script:phase = 'prepare'
    $script:failureCode = 'prepare_io'
    if (-not (Test-Path -LiteralPath $script:workRoot)) { New-Item -ItemType Directory -Path $script:workRoot | Out-Null }
    Assert-NoReparsePath $script:workRoot
    $script:runID = [guid]::NewGuid().ToString('N')
    $script:runRoot = Join-Path $script:workRoot "roster-selfhost-$script:runID"
    $script:privateDirectory = Join-Path $script:workRoot ".roster-selfhost-private-$script:runID"
    if ((Test-Path -LiteralPath $script:runRoot) -or (Test-Path -LiteralPath $script:privateDirectory)) { throw 'prepare' }
    Assert-ChildPath $script:runRoot $script:workRoot
    Assert-ChildPath $script:privateDirectory $script:workRoot
    New-Item -ItemType Directory -Path $script:runRoot | Out-Null
    New-Item -ItemType Directory -Path $script:privateDirectory | Out-Null
    Assert-NoReparsePath $script:privateDirectory
    $script:resultPath = Join-Path $script:runRoot 'result.json'
    $script:result.status = 'failed'
    $script:result.mode = 'bridge_fixture'
    $script:result.task_id = [string]$task.task_id
    $script:result.result_path = $script:resultPath
    $script:result.work_project = Join-Path $script:runRoot 'project'
    $script:result.planning_project = Join-Path $script:runRoot 'planning-project'
    $script:result.patch_path = Join-Path $script:runRoot 'changes.patch'
    $script:result.planning_patch_path = Join-Path $script:runRoot 'planning-changes.patch'
    $script:callID = 'bridge-' + [guid]::NewGuid().ToString('N')
    $script:result.models.requested[$Role] = $Model
    $script:result.bridge_fixture = [ordered]@{
        role = $Role; call_id = $script:callID; call_deadline = $null; technical_state = $null
        runtime_status = $null; runtime_events = @(); last_snapshot = $null; abort_ack = $null
        unresolved = $true; fixture_observation = [ordered]@{ thread_id = $null; usage = $null; exit_code = $null }
        phase_timings_ms = [ordered]@{ preflight = $null; build = $null; startup = $null }
    }
    $baseline = Join-Path $script:runRoot 'baseline'
    $project = $script:result.work_project
    $planningBaseline = Join-Path $script:runRoot 'planning-baseline'
    $planningProject = $script:result.planning_project
    Copy-GitSnapshot $targetPath $baseline $targetFiles $script:runRoot
    Copy-GitSnapshot $targetPath $project $targetFiles $script:runRoot
    Copy-GitSnapshot $planningPath $planningBaseline $planningFiles $script:runRoot
    Copy-GitSnapshot $planningPath $planningProject $planningFiles $script:runRoot
    [IO.File]::WriteAllBytes($script:result.patch_path, [byte[]]::new(0))
    [IO.File]::WriteAllBytes($script:result.planning_patch_path, [byte[]]::new(0))
    $normalizedTaskPath = Join-Path $script:runRoot 'task.json'
    [IO.File]::WriteAllText($normalizedTaskPath, (ConvertTo-Json -InputObject $task -Depth 80), [Text.UTF8Encoding]::new($false))
    $script:bridgeSnapshotPrepared = $true
    $privateScratch = Join-Path $script:privateDirectory 'scratch'
    New-Item -ItemType Directory -Path $privateScratch | Out-Null
    $script:phase = 'bridge'
    $script:failureCode = 'bridge_failed'
    $preflightDeadline = $script:bridgePreflightMilliseconds
    $script:failureCode = 'docker_unavailable'
    [void](Invoke-DockerBounded @('version', '--format', '{{.Server.Version}}') $preflightDeadline)

    $dockerValidationName = 'roster-fixture-check-' + $script:runID
    $script:ownedContainers.Add($dockerValidationName)
    $script:failureCode = 'fixture_cli_invalid'
    $fixtureMount = "${fixtureCliPath}:/fixture/cli:ro"
    $checkScript = 'const fs=require("node:fs");const s=fs.statSync("/fixture/cli");if(!s.isFile()||!(s.mode&0o111))process.exit(3)'
    [void](Invoke-DockerBounded @('run', '--name', $dockerValidationName, '--network', 'none', '-v', $fixtureMount, 'node:24-bookworm-slim', 'node', '-e', $checkScript) $preflightDeadline)
    [void](Invoke-DockerBounded @('rm', $dockerValidationName) $preflightDeadline)
    [void]$script:ownedContainers.Remove($dockerValidationName)

    $roleProject = if ($Role -eq 'planner') { '/planning' } else { '/project' }
    $roleMode = if ($Role -eq 'reviewer') { 'read-only' } else { 'workspace-write' }
    $projectAccess = if ($Role -eq 'implementer') { 'rw' } else { 'ro' }
    $planningAccess = if ($Role -eq 'planner') { 'rw' } else { 'ro' }
    $agentOptions = ConvertTo-Json -InputObject ([ordered]@{
        role = $Role; model = $Model; project = $roleProject; mode = $roleMode
        executable = '/fixture/cli'; timeout_seconds = $CallTimeoutSeconds
    }) -Compress
    $serviceToken = [guid]::NewGuid().ToString('N') + [guid]::NewGuid().ToString('N')
    $environmentPath = Join-Path $script:privateDirectory 'bridge.env'
    [IO.File]::WriteAllText($environmentPath, "ROSTER_TOKEN=$serviceToken`nRUNTIME_TOKEN=$serviceToken`nROSTER_AGENT_OPTIONS=$agentOptions`n", [Text.UTF8Encoding]::new($false))
    $configPath = Join-Path $script:privateDirectory 'core-config.json'
    $coreConfig = [ordered]@{
        nodes = @(@{ id = $Role; connector_id = 'runtime'; binding = @{ url = 'http://127.0.0.1:7331'; target_id = $Role; token_env = 'RUNTIME_TOKEN' } })
        subscriptions = @(); conversations = @()
    }
    Write-BridgeJson $configPath $coreConfig
    $emptyPath = Join-Path $script:privateDirectory 'empty.json'
    [IO.File]::WriteAllText($emptyPath, '{}', [Text.UTF8Encoding]::new($false))
    $modulePath = Join-Path $executorPath 'examples/selfhost-agents.mjs'
    if (-not (Test-Path -LiteralPath $modulePath -PathType Leaf)) { throw 'path' }
    Assert-NoReparsePath $modulePath

    if ((Get-BridgeRemainingMilliseconds $preflightDeadline) -le 0) { throw 'deadline' }
    $script:result.bridge_fixture.phase_timings_ms.preflight = [int][math]::Floor($script:bridgeWatch.Elapsed.TotalMilliseconds)
    $script:bridgeBuildMilliseconds = $script:bridgeWatch.Elapsed.TotalMilliseconds + $BuildTimeoutSeconds * 1000.0
    $buildDeadline = $script:bridgeBuildMilliseconds

    $builderName = 'roster-core-build-' + $script:runID
    $script:ownedContainers.Add($builderName)
    $script:failureCode = 'core_build_failed'
    $script:bridgeBuildStartedMilliseconds = $script:bridgeWatch.Elapsed.TotalMilliseconds
    [void](Invoke-DockerBounded @('run', '--name', $builderName, '--mount', "type=bind,source=$executorPath,target=/src,readonly", '--mount', "type=bind,source=$($script:privateDirectory),target=/out", '--mount', 'type=volume,source=roster-c3-gomod,target=/go/pkg/mod', '-w', '/src', '-e', 'GOTOOLCHAIN=local', 'golang:1.26', 'go', 'build', '-o', '/out/roster', './cmd/roster') $buildDeadline)
    $script:bridgeBuildDurationMilliseconds = [int][math]::Floor($script:bridgeWatch.Elapsed.TotalMilliseconds - $script:bridgeBuildStartedMilliseconds)
    $script:result.bridge_fixture.phase_timings_ms.build = $script:bridgeBuildDurationMilliseconds
    [void](Invoke-DockerBounded @('rm', $builderName) $buildDeadline)
    [void]$script:ownedContainers.Remove($builderName)

    $startupDeadline = [math]::Min($script:bridgeWatch.Elapsed.TotalMilliseconds + 60000.0, $script:bridgeOverallMilliseconds - ($CallTimeoutSeconds * 1000.0 + 60000.0))
    if ((Get-BridgeRemainingMilliseconds $startupDeadline) -le 0) { throw 'deadline' }
    $script:bridgeStartupStartedMilliseconds = $script:bridgeWatch.Elapsed.TotalMilliseconds
    $script:runtimeContainer = 'roster-runtime-' + $script:runID
    $script:ownedContainers.Add($script:runtimeContainer)
    $script:failureCode = 'runtime_start_failed'
    $runtimeArguments = @('run', '--detach', '--name', $script:runtimeContainer, '--network', 'none', '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges', '--env-file', $environmentPath,
        '-e', 'PORT=7331', '-e', 'ROSTER_LISTEN=127.0.0.1', '-e', 'ROSTER_RUNTIME_ID=fixture-runtime', '-e', 'ROSTER_DATA_DIR=/scratch/runtime-data',
        '-e', 'ROSTER_AGENT_MODULE=/executor/examples/selfhost-agents.mjs', '-v', "${runtimePath}:/runtime:ro", '-v', "${executorPath}:/executor:ro", '-w', '/runtime',
        '-v', "${fixtureCliPath}:/fixture/cli:ro", '-v', "${normalizedTaskPath}:/fixture/task.json:ro", '-v', "$($script:result.work_project):/project:$projectAccess",
        '-v', "${planningProject}:/planning:$planningAccess", '-v', "${privateScratch}:/scratch:rw", 'node:24-bookworm-slim', 'node', 'dist/main.js')
    [void](Invoke-DockerBounded $runtimeArguments $startupDeadline)
    $runtimeReadyScript = 'const r=await fetch("http://127.0.0.1:7331/health");if(!r.ok)process.exit(1)'
    $runtimeReady = $false
    while (-not $runtimeReady -and (Get-BridgeRemainingMilliseconds $startupDeadline) -gt 0) {
        try { [void](Invoke-DockerBounded @('exec', $script:runtimeContainer, 'node', '--input-type=module', '-e', $runtimeReadyScript) $startupDeadline); $runtimeReady = $true }
        catch { Start-Sleep -Milliseconds ([math]::Min(250, [math]::Max(1, (Get-BridgeRemainingMilliseconds $startupDeadline)))) }
    }
    if (-not $runtimeReady) { throw 'startup' }

    $script:coreContainer = 'roster-core-' + $script:runID
    $script:ownedContainers.Add($script:coreContainer)
    $script:failureCode = 'core_start_failed'
    [void](Invoke-DockerBounded @('run', '--detach', '--name', $script:coreContainer, '--network', "container:$($script:runtimeContainer)", '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges', '--env-file', $environmentPath,
        '-v', "${script:privateDirectory}:/private:ro", 'node:24-bookworm-slim', '/private/roster', 'serve', '--listen', '127.0.0.1:7350', '--config', '/private/core-config.json') $startupDeadline)
    $coreReady = $false
    while (-not $coreReady -and (Get-BridgeRemainingMilliseconds $startupDeadline) -gt 0) {
        try {
            $status = Invoke-DockerBounded @('exec', $script:coreContainer, '/private/roster', 'core.status', '--file', '/private/empty.json') $startupDeadline
            $statusObject = ConvertFrom-Json -InputObject $status -Depth 20 -ErrorAction Stop
            $coreReady = $statusObject.lifecycle.value.state -eq 'running'
        } catch { Start-Sleep -Milliseconds ([math]::Min(250, [math]::Max(1, (Get-BridgeRemainingMilliseconds $startupDeadline)))) }
    }
    if (-not $coreReady) { throw 'startup' }
    $script:bridgeStartupDurationMilliseconds = [int][math]::Floor($script:bridgeWatch.Elapsed.TotalMilliseconds - $script:bridgeStartupStartedMilliseconds)
    $script:result.bridge_fixture.phase_timings_ms.startup = $script:bridgeStartupDurationMilliseconds

    $script:failureCode = 'call_failed'
    $callID = $script:callID
    $callDeadline = [DateTime]::UtcNow.AddSeconds($CallTimeoutSeconds)
    $script:result.bridge_fixture.call_deadline = $callDeadline.ToString('o')
    $script:bridgePhaseDeadline = [math]::Min($script:bridgeWatch.Elapsed.TotalMilliseconds + $CallTimeoutSeconds * 1000.0, $script:bridgeOverallMilliseconds - 60000.0)
    if ($script:bridgePhaseDeadline -le $script:bridgeWatch.Elapsed.TotalMilliseconds) { throw 'deadline' }
    $requestObject = [ordered]@{
        task_id = [string]$task.task_id; role = $Role; goal = [string]$task.goal
        requirements = @($task.requirements); planning_files = @($task.planning_files); allowed_files = @($task.allowed_files)
    }
    $requestText = ConvertTo-Json -InputObject $requestObject -Depth 20 -Compress
    $submitInput = [ordered]@{
        id = $callID; target = @{ namespace = 'nodes'; id = $Role }; operation = 'run'
        input = @{ request = $requestText }; deadline = $callDeadline.ToString('o')
    }
    $submitPath = Join-Path $script:privateDirectory 'submit.json'
    $getPath = Join-Path $script:privateDirectory 'get.json'
    $inspectPath = Join-Path $script:privateDirectory 'inspect.json'
    $abortPath = Join-Path $script:privateDirectory 'abort.json'
    Write-BridgeJson $script:resultPath $script:result
    $lastSnapshot = $null
    try {
        $lastSnapshot = Invoke-CoreCommand 'calls.submit' $submitInput $submitPath $script:bridgePhaseDeadline
        if ([string]$lastSnapshot.value.call.id -ne $callID) { throw 'wire' }
        $script:result.bridge_fixture.submit_response_observed = $true
    } catch { $script:result.bridge_fixture.submit_response_observed = $false }

    $lastInspectAt = -1.0
    while ((Get-BridgeRemainingMilliseconds $script:bridgePhaseDeadline) -gt 0) {
        try {
            $current = Invoke-CoreCommand 'calls.get' @{ id = $callID } $getPath $script:bridgePhaseDeadline
            if (-not $current.value.call.id -or [string]$current.value.call.id -ne $callID -or -not $current.value.lifecycle.state) { throw 'wire' }
            $lastSnapshot = $current
            $state = [string]$current.value.lifecycle.state
            if ($state -in @('rejected', 'succeeded', 'failed', 'cancelled', 'expired')) { break }
            if ($state -in @('unknown', 'reconciling') -and ($script:bridgeWatch.Elapsed.TotalMilliseconds - $lastInspectAt) -ge 1000) {
                try {
                    $current = Invoke-CoreCommand 'calls.inspect' @{ id = $callID } $inspectPath $script:bridgePhaseDeadline
                    if ([string]$current.value.call.id -eq $callID) { $lastSnapshot = $current }
                } catch { }
                $lastInspectAt = $script:bridgeWatch.Elapsed.TotalMilliseconds
            }
        } catch { }
        if ($null -ne $lastSnapshot -and [string]$lastSnapshot.value.lifecycle.state -in @('rejected', 'succeeded', 'failed', 'cancelled', 'expired')) { break }
        $remaining = Get-BridgeRemainingMilliseconds $script:bridgePhaseDeadline
        if ($remaining -gt 0) { Start-Sleep -Milliseconds ([math]::Min(250, $remaining)) }
    }

    $adjustDeadline = [math]::Min($script:bridgePhaseDeadline + 30000.0, $script:bridgeOverallMilliseconds - 30000.0)
    if ($null -ne $lastSnapshot -and [string]$lastSnapshot.value.lifecycle.state -notin @('rejected', 'succeeded', 'failed', 'cancelled', 'expired')) {
        try {
            $current = Invoke-CoreCommand 'calls.inspect' @{ id = $callID } $inspectPath $adjustDeadline
            if ([string]$current.value.call.id -eq $callID) { $lastSnapshot = $current }
        } catch { }
        if ($null -ne $lastSnapshot -and [string]$lastSnapshot.value.lifecycle.state -notin @('rejected', 'succeeded', 'failed', 'cancelled', 'expired') -and (Get-BridgeRemainingMilliseconds $adjustDeadline) -gt 0) {
            try {
                $abortSnapshot = Invoke-CoreCommand 'calls.abort' @{ id = $callID; reason = 'bridge_fixture_deadline' } $abortPath $adjustDeadline
                if ([string]$abortSnapshot.value.call.id -eq $callID -and
                    [string]$abortSnapshot.value.lifecycle.state -in @('rejected', 'succeeded', 'failed', 'cancelled', 'expired')) {
                    $lastSnapshot = $abortSnapshot
                }
                $script:result.bridge_fixture.abort_ack = [ordered]@{
                    abort_requested = [bool]$abortSnapshot.value.abort_requested
                    technical_state = [string]$abortSnapshot.value.lifecycle.state
                    result_state = if ($null -ne $abortSnapshot.value.result) { [string]$abortSnapshot.value.result.state } else { $null }
                }
            } catch { $script:result.bridge_fixture.abort_ack = @{ received = $false } }
            if ((Get-BridgeRemainingMilliseconds $adjustDeadline) -gt 0) {
                try {
                    $current = Invoke-CoreCommand 'calls.inspect' @{ id = $callID } $inspectPath $adjustDeadline
                    if ([string]$current.value.call.id -eq $callID) { $lastSnapshot = $current }
                } catch { }
            }
        }
    }
    if ($null -ne $lastSnapshot) {
        $script:result.bridge_fixture.last_snapshot = $lastSnapshot
        $script:result.bridge_fixture.technical_state = [string]$lastSnapshot.value.lifecycle.state
        $script:result.bridge_fixture.unresolved = [string]$lastSnapshot.value.lifecycle.state -notin @('rejected', 'succeeded', 'failed', 'cancelled', 'expired')
        $result = $lastSnapshot.value.result
        if ($null -ne $result -and $result.output) {
            if ($result.output -is [System.Collections.IDictionary]) {
                $script:result.bridge_fixture.runtime_status = $result.output['status']
                if ($result.output.Contains('events')) { $script:result.bridge_fixture.runtime_events = @($result.output['events']) }
            } else {
                $script:result.bridge_fixture.runtime_status = $result.output.status
                if ($null -ne $result.output.events) { $script:result.bridge_fixture.runtime_events = @($result.output.events) }
            }
        }
        if ($null -ne $lastSnapshot.value.result -and $lastSnapshot.value.result.output) {
            $output = $lastSnapshot.value.result.output
            if ($output -is [System.Collections.IDictionary] -and $output['output']) { $output = $output['output'] }
            elseif ($output.output) { $output = $output.output }
            if ($output -is [System.Collections.IDictionary]) {
                $script:result.bridge_fixture.fixture_observation.thread_id = $output['thread_id']
                $script:result.bridge_fixture.fixture_observation.usage = $output['usage']
                $script:result.bridge_fixture.fixture_observation.exit_code = $output['exit_code']
            } else {
                $script:result.bridge_fixture.fixture_observation.thread_id = $output.thread_id
                $script:result.bridge_fixture.fixture_observation.usage = $output.usage
                $script:result.bridge_fixture.fixture_observation.exit_code = $output.exit_code
            }
        }
        $script:result.status = 'observed'
    }
}

if ($BridgeFixture) {
    try {
        Invoke-BridgeFixture
    } catch {
        if ($script:result.bridge_fixture -and $null -ne $script:bridgeBuildStartedMilliseconds -and $null -eq $script:bridgeBuildDurationMilliseconds) {
            $script:bridgeBuildDurationMilliseconds = [int][math]::Floor($script:bridgeWatch.Elapsed.TotalMilliseconds - $script:bridgeBuildStartedMilliseconds)
            $script:result.bridge_fixture.phase_timings_ms.build = $script:bridgeBuildDurationMilliseconds
        }
        if ($script:result.bridge_fixture -and $null -ne $script:bridgeStartupStartedMilliseconds -and $null -eq $script:bridgeStartupDurationMilliseconds) {
            $script:bridgeStartupDurationMilliseconds = [int][math]::Floor($script:bridgeWatch.Elapsed.TotalMilliseconds - $script:bridgeStartupStartedMilliseconds)
            $script:result.bridge_fixture.phase_timings_ms.startup = $script:bridgeStartupDurationMilliseconds
        }
        $script:result.status = 'failed'
        $stage = $script:phase
        $code = $script:failureCode
        if ($_.Exception.Message -eq 'path') { $stage = 'path'; $code = 'path_invalid' }
        elseif ($_.Exception.Message -eq 'task') { $stage = 'task'; $code = 'task_invalid' }
        elseif ($_.Exception.Message -eq 'deadline') { $stage = 'bridge'; $code = 'bridge_deadline' }
        elseif ($_.Exception.Message -eq 'wire') { $stage = 'bridge'; $code = 'bridge_protocol' }
        elseif ($_.Exception.Message -eq 'patch') { $stage = 'changes'; $code = 'change_capture_failed' }
        $script:result.error = [ordered]@{ stage = $stage; code = $code }
    } finally {
        $cleanupErrors = [Collections.Generic.List[string]]::new()
        $cleanupDeadline = if ($script:bridgeWatch) { $script:bridgeWatch.Elapsed.TotalMilliseconds + 30000.0 } else { 30000.0 }
        for ($index = $script:ownedContainers.Count - 1; $index -ge 0; $index--) {
            try {
                [void](Invoke-DockerBounded @('rm', '--force', $script:ownedContainers[$index]) $cleanupDeadline)
                $script:ownedContainers.RemoveAt($index)
            } catch { $cleanupErrors.Add('container_cleanup_failed') }
        }
        $containersStopped = $script:ownedContainers.Count -eq 0
        if ($containersStopped -and $script:bridgeSnapshotPrepared -and $script:runRoot -and (Test-Path -LiteralPath $script:result.work_project -PathType Container)) {
            try {
                $codeChanges = @(Get-SnapshotChanges (Join-Path $script:runRoot 'baseline') $script:result.work_project $cleanupDeadline)
                $planningChanges = @(Get-SnapshotChanges (Join-Path $script:runRoot 'planning-baseline') $script:result.planning_project $cleanupDeadline)
                $script:result.changed_files = @($codeChanges | ForEach-Object { "project/$_" }) + @($planningChanges | ForEach-Object { "planning/$_" })
                $allowed = [Collections.Generic.HashSet[string]]::new((Get-PathStringComparer))
                $taskObject = Get-Content -LiteralPath (Join-Path $script:runRoot 'task.json') -Raw | ConvertFrom-Json -Depth 80
                $permitted = if ($Role -eq 'planner') { @($taskObject.planning_files | ForEach-Object { "planning/$_" }) } elseif ($Role -eq 'implementer') { @($taskObject.allowed_files | ForEach-Object { "project/$_" }) } else { @() }
                foreach ($path in $permitted) { [void]$allowed.Add($path) }
                if (@($script:result.changed_files | Where-Object { -not $allowed.Contains($_) }).Count -gt 0) {
                    $script:result.status = 'failed'
                    $script:result.error = [ordered]@{ stage = 'changes'; code = 'change_outside_allowlist' }
                }
                [IO.File]::WriteAllText($script:result.patch_path, (Invoke-GitDirectoryDiff (Join-Path $script:runRoot 'baseline') $script:result.work_project $script:runRoot $cleanupDeadline), [Text.UTF8Encoding]::new($false))
                [IO.File]::WriteAllText($script:result.planning_patch_path, (Invoke-GitDirectoryDiff (Join-Path $script:runRoot 'planning-baseline') $script:result.planning_project $script:runRoot $cleanupDeadline), [Text.UTF8Encoding]::new($false))
            } catch {
                if ($null -eq $script:result.error) {
                    $script:result.status = 'failed'
                    $script:result.error = [ordered]@{ stage = 'changes'; code = 'change_capture_failed' }
                }
            }
        } elseif (-not $containersStopped) { $cleanupErrors.Add('containers_may_be_running') }
        if ($containersStopped) {
            try { Remove-OwnedPrivateDirectory $cleanupDeadline } catch { $cleanupErrors.Add('private_cleanup_failed') }
        }
        $script:result.cleanup.complete = $cleanupErrors.Count -eq 0
        $script:result.cleanup.errors = @($cleanupErrors.ToArray() | Select-Object -Unique)
        if ($cleanupErrors.Count -gt 0) {
            $script:result.status = 'failed'
            $script:result.error = [ordered]@{ stage = 'cleanup'; code = 'cleanup_failed' }
        }
        $script:result.completed_at = [DateTime]::UtcNow.ToString('o')
        $resultWritten = $false
        if ($script:resultPath -and (Test-Path -LiteralPath $script:runRoot -PathType Container)) {
            try { Write-BridgeJson $script:resultPath $script:result; $resultWritten = $true }
            catch {
                $script:result.status = 'failed'
                $script:result.error = [ordered]@{ stage = 'result'; code = 'result_write_failed' }
                [Console]::Error.WriteLine('result:result_write_failed')
            }
        }
        if ($script:result.status -eq 'failed' -and $script:result.error) { [Console]::Error.WriteLine("$($script:result.error.stage):$($script:result.error.code)") }
    }
    if ($resultWritten) { Write-Output $script:resultPath }
    if ($script:result.status -eq 'failed') { exit 1 }
} else {
try {
    $script:phase = 'path'
    $script:failureCode = 'path_invalid'
    foreach ($value in @($TargetRepository, $PlanningRepository, $TaskFile, $WorkDirectory)) {
        if ([string]::IsNullOrWhiteSpace($value)) { throw 'path' }
    }
    $targetPath = Get-FullPath $TargetRepository
    $planningPath = Get-FullPath $PlanningRepository
    $taskPath = Get-FullPath $TaskFile
    $script:workRoot = Get-FullPath $WorkDirectory

    foreach ($repository in @($targetPath, $planningPath)) {
        if (-not (Test-Path -LiteralPath $repository -PathType Container)) { throw 'path' }
        Assert-NoReparsePath $repository
        $gitRoot = Get-GitRoot $repository
        if (-not $gitRoot.Equals((Get-ComparablePath $repository), [StringComparison]::OrdinalIgnoreCase)) { throw 'path' }
    }
    if (Test-PathOverlap $targetPath $planningPath) { throw 'path' }
    if (Test-Path -LiteralPath $script:workRoot -PathType Leaf) { throw 'path' }
    Assert-NoReparsePath $script:workRoot -AllowMissing
    if ((Test-PathOverlap $script:workRoot $targetPath) -or (Test-PathOverlap $script:workRoot $planningPath)) { throw 'path' }
    if (-not (Test-Path -LiteralPath $taskPath -PathType Leaf)) { throw 'path' }
    Assert-NoReparsePath $taskPath

    $script:phase = 'task'
    $script:failureCode = 'task_invalid'
    $taskText = [IO.File]::ReadAllText($taskPath, [Text.Encoding]::UTF8)
    $task = ConvertFrom-Json -InputObject $taskText -Depth 60 -ErrorAction Stop
    if ($null -eq $task -or $task -is [array]) { throw 'task' }
    foreach ($key in @('task_id', 'goal', 'requirements', 'planning_files', 'allowed_files', 'required_verification')) {
        if ($null -eq $task.$key) { throw 'task' }
    }
    if ([string]$task.task_id -notmatch '^[A-Za-z0-9][A-Za-z0-9_.-]{0,39}$' -or [string]::IsNullOrWhiteSpace([string]$task.goal)) { throw 'task' }
    if ($task.requirements -isnot [array] -or $task.requirements.Count -lt 1 -or $task.planning_files -isnot [array] -or $task.allowed_files -isnot [array] -or $task.allowed_files.Count -lt 1 -or $task.required_verification -isnot [array] -or $task.required_verification.Count -lt 1) { throw 'task' }
    foreach ($requirement in $task.requirements) { if ($requirement -isnot [string] -or [string]::IsNullOrWhiteSpace($requirement)) { throw 'task' } }
    foreach ($check in $task.required_verification) {
        if (-not $check.image -or [string]$check.image -notmatch '^[A-Za-z0-9][A-Za-z0-9._/:@-]{0,200}$' -or $check.command -isnot [array] -or $check.command.Count -lt 1) { throw 'task' }
        foreach ($argument in $check.command) { if ($argument -isnot [string] -or [string]::IsNullOrWhiteSpace($argument)) { throw 'task' } }
    }

    $targetFiles = @(Get-TrackedFiles $targetPath)
    $planningFiles = @(Get-TrackedFiles $planningPath)
    $planningAllowed = [Collections.Generic.List[string]]::new()
    $planningSet = [Collections.Generic.HashSet[string]]::new((Get-PathStringComparer))
    foreach ($path in $task.planning_files) {
        $relative = Assert-TrackedFile $planningPath $planningFiles $path
        if (-not $planningSet.Add($relative)) { throw 'task' }
        $planningAllowed.Add($relative)
    }
    $codeAllowed = [Collections.Generic.List[string]]::new()
    $codeSet = [Collections.Generic.HashSet[string]]::new((Get-PathStringComparer))
    foreach ($path in $task.allowed_files) {
        $relative = Assert-TrackedFile $targetPath $targetFiles $path
        if (-not $codeSet.Add($relative)) { throw 'task' }
        $codeAllowed.Add($relative)
    }
    $task.planning_files = $planningAllowed.ToArray()
    $task.allowed_files = $codeAllowed.ToArray()

    $script:phase = 'prepare'
    $script:failureCode = 'prepare_io'
    if (-not (Test-Path -LiteralPath $script:workRoot)) { New-Item -ItemType Directory -Path $script:workRoot | Out-Null }
    Assert-NoReparsePath $script:workRoot
    $script:runID = [guid]::NewGuid().ToString('N')
    $script:runRoot = Join-Path $script:workRoot "roster-selfhost-$script:runID"
    $script:privateDirectory = Join-Path $script:workRoot ".roster-selfhost-private-$script:runID"
    if ((Test-Path -LiteralPath $script:runRoot) -or (Test-Path -LiteralPath $script:privateDirectory)) { throw 'prepare' }
    Assert-ChildPath $script:runRoot $script:workRoot
    Assert-ChildPath $script:privateDirectory $script:workRoot
    New-Item -ItemType Directory -Path $script:runRoot | Out-Null
    Assert-NoReparsePath $script:runRoot
    New-Item -ItemType Directory -Path $script:privateDirectory | Out-Null
    Assert-NoReparsePath $script:privateDirectory

    $baseline = Join-Path $script:runRoot 'baseline'
    $project = Join-Path $script:runRoot 'project'
    $planningBaseline = Join-Path $script:runRoot 'planning-baseline'
    $planningProject = Join-Path $script:runRoot 'planning-project'
    $script:resultPath = Join-Path $script:runRoot 'result.json'
    $patchPath = Join-Path $script:runRoot 'changes.patch'
    $planningPatchPath = Join-Path $script:runRoot 'planning-changes.patch'
    $normalizedTaskPath = Join-Path $script:runRoot 'task.json'
    $script:result.task_id = [string]$task.task_id
    $script:result.work_project = $project
    $script:result.planning_project = $planningProject
    $script:result.patch_path = $patchPath
    $script:result.planning_patch_path = $planningPatchPath
    $script:result.result_path = $script:resultPath

    Copy-GitSnapshot $targetPath $baseline $targetFiles $script:runRoot
    Copy-GitSnapshot $targetPath $project $targetFiles $script:runRoot
    Copy-GitSnapshot $planningPath $planningBaseline $planningFiles $script:runRoot
    Copy-GitSnapshot $planningPath $planningProject $planningFiles $script:runRoot
    [IO.File]::WriteAllBytes($patchPath, [byte[]]::new(0))
    [IO.File]::WriteAllBytes($planningPatchPath, [byte[]]::new(0))
    $normalizedTask = ConvertTo-Json -InputObject $task -Depth 80
    [IO.File]::WriteAllText($normalizedTaskPath, $normalizedTask, [Text.UTF8Encoding]::new($false))
    $script:result.status = 'prepared'
} catch {
    $script:result.status = 'failed'
    if ($_.Exception.Message -eq 'path') {
        $script:phase = 'path'
        $script:failureCode = 'path_invalid'
    } elseif ($_.Exception.Message -eq 'task') {
        $script:phase = 'task'
        $script:failureCode = 'task_invalid'
    }
    $script:result.error = [ordered]@{ stage = $script:phase; code = $script:failureCode }
} finally {
    try {
        Remove-OwnedPrivateDirectory
        $script:result.cleanup.complete = $true
        $script:result.cleanup.errors = @()
    } catch {
        $script:result.status = 'failed'
        $script:result.error = [ordered]@{ stage = 'cleanup'; code = 'cleanup_failed' }
        $script:result.cleanup.complete = $false
        $script:result.cleanup.errors = @('cleanup_failed')
    }
    $script:result.completed_at = [DateTime]::UtcNow.ToString('o')
    $resultWritten = $false
    if ($script:resultPath) {
        try {
            $resultJson = ConvertTo-Json -InputObject $script:result -Depth 80
            [IO.File]::WriteAllText($script:resultPath, $resultJson, [Text.UTF8Encoding]::new($false))
            $resultWritten = $true
        } catch {
            $script:result.status = 'failed'
            $script:result.error = [ordered]@{ stage = 'prepare'; code = 'prepare_io' }
        }
    }
    if ($script:result.status -ne 'prepared' -and $script:result.error) { [Console]::Error.WriteLine("$($script:result.error.stage):$($script:result.error.code)") }
}

if ($resultWritten) { Write-Output $script:resultPath }
if ($script:result.status -ne 'prepared') {
    exit 1
}
}
