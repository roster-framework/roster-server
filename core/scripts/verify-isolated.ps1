param(
    [string]$Docker = 'docker',
    [string]$WorkDirectory = (Join-Path ([IO.Path]::GetTempPath()) 'roster-integration-tests'),
    [string]$CacheDirectory = '',
    [string]$RuntimeRepository,
    [string]$ResourceRepository,
    [ValidateRange(1, 300)][int]$ReadinessTimeoutSeconds = 60
)
$ErrorActionPreference = 'Stop'
$core = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$serverRoot = Split-Path $core -Parent
$workspaceRoot = Split-Path $serverRoot -Parent
$clientRoot = Join-Path $workspaceRoot 'roster-node-client'
if (-not $RuntimeRepository) { $RuntimeRepository = Join-Path $clientRoot 'runtime' }
if (-not $ResourceRepository) { $ResourceRepository = Join-Path $clientRoot 'resource' }
if (-not (Test-Path (Join-Path $RuntimeRepository 'dist/main.js'))) { throw 'Build roster-node-client/runtime first (dist and node_modules must exist).' }
$runID = [guid]::NewGuid().ToString('N')
$work = [IO.Path]::GetFullPath((Join-Path $WorkDirectory "roster-$runID"))
New-Item -ItemType Directory -Path $work -Force | Out-Null
$environment = Join-Path $work 'isolated.env'
$authorityPolicy = Join-Path $work 'authority.json'
$runtimeToken = [guid]::NewGuid().ToString('N') + [guid]::NewGuid().ToString('N')
$resourceToken = [guid]::NewGuid().ToString('N') + [guid]::NewGuid().ToString('N')
$adminToken = [guid]::NewGuid().ToString('N') + [guid]::NewGuid().ToString('N')
@("ROSTER_TOKEN=$runtimeToken", "RUNTIME_TOKEN=$runtimeToken", "RESOURCE_TOKEN=$resourceToken", "ROSTER_RESOURCE_TOKEN=$resourceToken", "ROSTER_RESOURCE_ADMIN_TOKEN=$adminToken") | Set-Content -LiteralPath $environment -Encoding utf8
$principals = @(@{ namespace = 'operators'; id = 'local' }, @{ namespace = 'nodes'; id = 'designer' }, @{ namespace = 'nodes'; id = 'developer' }, @{ namespace = 'nodes'; id = 'notes' })
$targets = @(@{ id = 'designer'; operations = @('run') }, @{ id = 'developer'; operations = @('run') }, @{ id = 'notes'; operations = @('read','publish') })
$grants = [Collections.Generic.List[object]]::new()
foreach ($target in $targets) {
    foreach ($principal in $principals) {
        $grants.Add(@{ principal = $principal; action = 'invoke'; target = @{ namespace = 'nodes'; id = $target.id }; operations = $target.operations })
    }
    $grants.Add(@{ principal = $principals[0]; action = 'abort'; target = @{ namespace = 'nodes'; id = $target.id }; operations = $target.operations })
}
@{ schema = 'roster-authority/1'; grants = $grants.ToArray() } | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $authorityPolicy -Encoding utf8NoBOM
$started = [Collections.Generic.List[string]]::new()
function Invoke-Docker([string[]]$Arguments) {
    $output = & $Docker @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Docker action failed: $($Arguments[0])" }
    return $output
}
function Build-Binary([string]$Repository, [string]$Output, [string]$Package) {
    $arguments = @('run','--rm','-v',"${Repository}:/src:ro",'-v',"${work}:/out",'-w','/src','-e','GOTOOLCHAIN=local','-e','CGO_ENABLED=0')
    if ($CacheDirectory -and (Test-Path (Join-Path $CacheDirectory 'go-mod-cache'))) {
        $arguments += @('--network','none','-v',"${CacheDirectory}/go-mod-cache:/go/pkg/mod",'-v',"${CacheDirectory}/go-build-cache:/root/.cache/go-build",'-e','GOPROXY=off','-e','GOSUMDB=off')
    }
    $arguments += @('golang:1.26.10','go','build','-o',"/out/$Output",$Package)
    Invoke-Docker $arguments | Out-Null
}
function Start-Container([string]$Name,[string[]]$Arguments) {
    $started.Add($Name)
    Invoke-Docker (@('run','-d','--name',$Name) + $Arguments) | Out-Null
}
function Invoke-DockerBounded([string[]]$Arguments,[int]$TimeoutMilliseconds) {
    if ($TimeoutMilliseconds -lt 1) { return [pscustomobject]@{ TimedOut = $true; ExitCode = -1; Output = '' } }
    $startInfo = [Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $Docker
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    foreach ($argument in $Arguments) { [void]$startInfo.ArgumentList.Add([string]$argument) }
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    $clock = [Diagnostics.Stopwatch]::StartNew()
    try {
        if (-not $process.Start()) { return [pscustomobject]@{ TimedOut = $false; ExitCode = -1; Output = '' } }
        $stdout = $process.StandardOutput.ReadToEndAsync()
        $stderr = $process.StandardError.ReadToEndAsync()
        $remaining = [Math]::Max(0, $TimeoutMilliseconds - [int]$clock.ElapsedMilliseconds)
        if ($remaining -lt 1 -or -not $process.WaitForExit($remaining)) {
            try { $process.Kill($true) } catch {}
            return [pscustomobject]@{ TimedOut = $true; ExitCode = -1; Output = '' }
        }
        $remaining = [Math]::Max(0, $TimeoutMilliseconds - [int]$clock.ElapsedMilliseconds)
        $pipes = [Threading.Tasks.Task[]]@($stdout, $stderr)
        if ($remaining -lt 1 -or -not [Threading.Tasks.Task]::WaitAll($pipes, $remaining)) {
            try { $process.Kill($true) } catch {}
            return [pscustomobject]@{ TimedOut = $true; ExitCode = -1; Output = '' }
        }
        # Deliberately discard stderr. Docker may include paths or other local
        # details there; callers only interpret the bounded status protocol.
        [void]$stderr.GetAwaiter().GetResult()
        return [pscustomobject]@{ TimedOut = $false; ExitCode = $process.ExitCode; Output = $stdout.GetAwaiter().GetResult() }
    } catch {
        try { if (-not $process.HasExited) { $process.Kill($true) } } catch {}
        return [pscustomobject]@{ TimedOut = $false; ExitCode = -1; Output = '' }
    } finally {
        $process.Dispose()
    }
}
function Start-ReadinessContainer([string]$Name,[string[]]$Arguments) {
    $script:started.Add($Name)
    $remaining = Get-ReadinessRemainingMilliseconds
    if ($remaining -lt 1) { throw (Format-ReadinessFailure 'core_start' 0 'deadline') }
    $result = Invoke-DockerBounded (@('run','-d','--name',$Name) + $Arguments) $remaining
    if ($result.TimedOut) { throw (Format-ReadinessFailure 'core_start' 0 'timeout') }
    if ($result.ExitCode -ne 0) { throw (Format-ReadinessFailure 'core_start' 0 'docker_run') }
}
function Get-ReadinessRemainingMilliseconds {
    return [Math]::Max(0, [int]($script:readinessBudgetMilliseconds - $script:readinessClock.ElapsedMilliseconds))
}
function Get-ReadinessRoleState([string]$Role) {
    $remaining = Get-ReadinessRemainingMilliseconds
    if ($remaining -lt 1 -or -not $script:readinessRoles.ContainsKey($Role)) { return $null }
    $timeout = [Math]::Min(1000, $remaining)
    $name = $script:readinessRoles[$Role]
    $result = Invoke-DockerBounded @('inspect','--format','{{.State.Running}}|{{.State.ExitCode}}|{{.State.OOMKilled}}',$name) $timeout
    if ($result.TimedOut -or $result.ExitCode -ne 0) { return $null }
    if ($result.Output.Trim() -match '^(true|false)\|([0-9]+)\|(true|false)$') {
        return [pscustomobject]@{ Running = ($Matches[1] -eq 'true'); ExitCode = [int]$Matches[2]; OOMKilled = ($Matches[3] -eq 'true') }
    }
    return $null
}
function Format-ReadinessFailure([string]$Stage,[int]$Status,[string]$NormalizedError) {
    $roles = [Collections.Generic.List[string]]::new()
    foreach ($role in @('runtime','resource','core')) {
        $state = Get-ReadinessRoleState $role
        if ($null -eq $state) { $roles.Add("${role}=unavailable") }
        else { $roles.Add("${role}=running:$($state.Running),exitCode:$($state.ExitCode),oom:$($state.OOMKilled)") }
    }
    $elapsed = $script:readinessClock.ElapsedMilliseconds
    return "Readiness failed stage=$Stage elapsed_ms=$elapsed budget_seconds=$ReadinessTimeoutSeconds status=$Status error=$NormalizedError roles=$($roles -join ';')"
}
function Assert-ReadinessRolesAlive([string]$Stage,[int]$Status) {
    foreach ($role in @('runtime','resource','core')) {
        if (-not $script:readinessRoles.ContainsKey($role)) { continue }
        $state = Get-ReadinessRoleState $role
        if ($null -eq $state) { throw (Format-ReadinessFailure $Stage $Status 'role_inspect') }
        if (-not $state.Running -or $state.OOMKilled) { throw (Format-ReadinessFailure $Stage $Status 'container_exited') }
    }
}
$readinessProbe = @'
const [stage,timeoutText]=process.argv.slice(1);
const timeout=Math.max(1,Number(timeoutText));
const definitions={
  runtime:{url:'http://127.0.0.1:7351/state',token:process.env.ROSTER_TOKEN},
  resource:{url:'http://127.0.0.1:7352/v1/resources',token:process.env.ROSTER_RESOURCE_TOKEN},
  core:{url:'http://127.0.0.1:7350/ready',token:process.env.ROSTER_TOKEN}
};
const target=definitions[stage];
if(!target){process.stdout.write('0|0|protocol');process.exit(0);}
try {
  const response=await fetch(target.url,{headers:{Authorization:'Bearer '+target.token},signal:AbortSignal.timeout(timeout)});
  let ready=response.status===200;
  let error='none';
  if(stage==='core'&&response.status===200){
    try { const body=await response.json(); ready=body?.ready===true; if(!ready)error='not_ready'; }
    catch { ready=false; error='invalid_json'; }
  }
  process.stdout.write(`${response.status}|${ready?'1':'0'}|${error}`);
} catch(error) {
  process.stdout.write(`0|0|${error?.name==='TimeoutError'||error?.name==='AbortError'?'timeout':'network'}`);
}
'@
$seedResource = @'
const timeout=Math.max(1,Number(process.argv[1]));
try {
  const response=await fetch('http://127.0.0.1:7352/v1/resources',{method:'POST',headers:{Authorization:'Bearer '+process.env.ROSTER_RESOURCE_ADMIN_TOKEN,'Content-Type':'application/json'},body:JSON.stringify({id:'notes',name:'Notes',component:'document',version:0}),signal:AbortSignal.timeout(timeout)});
  process.stdout.write(`${response.status}|none`);
} catch(error) {
  process.stdout.write(`0|${error?.name==='TimeoutError'||error?.name==='AbortError'?'timeout':'network'}`);
}
'@
function Wait-ReadinessStage([string]$Stage) {
    while ($true) {
        Assert-ReadinessRolesAlive $Stage 0
        $remaining = Get-ReadinessRemainingMilliseconds
        if ($remaining -lt 1) { throw (Format-ReadinessFailure $Stage 0 'deadline') }
        $requestTimeout = [Math]::Min(2000, $remaining)
        $result = Invoke-DockerBounded @('exec',$script:readinessRoles[$Stage],'node','--input-type=module','-e',$readinessProbe,$Stage,[string]$requestTimeout) $remaining
        $status = 0
        $ready = $false
        $normalizedError = 'docker_exec'
        if ($result.TimedOut) { $normalizedError = 'timeout' }
        elseif ($result.ExitCode -eq 0 -and $result.Output.Trim() -match '^(\d{1,3})\|([01])\|([a-z_]+)$') {
            $status = [int]$Matches[1]
            $ready = $Matches[2] -eq '1'
            $normalizedError = $Matches[3]
        }
        if ($status -ge 400 -and $status -lt 500) { throw (Format-ReadinessFailure $Stage $status 'http_4xx') }
        if ($ready -and $status -eq 200) {
            Assert-ReadinessRolesAlive $Stage $status
            Write-Host "Readiness stage=$Stage status=200 elapsed_ms=$($script:readinessClock.ElapsedMilliseconds) budget_seconds=$ReadinessTimeoutSeconds"
            return
        }
        $remaining = Get-ReadinessRemainingMilliseconds
        if ($remaining -lt 1) { throw (Format-ReadinessFailure $Stage $status $normalizedError) }
        [Threading.Thread]::Sleep([Math]::Min(250, $remaining))
    }
}
function Invoke-SeedOnce {
    $remaining = Get-ReadinessRemainingMilliseconds
    if ($remaining -lt 1) { throw (Format-ReadinessFailure 'seed' 0 'deadline') }
    $requestTimeout = [Math]::Min(2000, $remaining)
    $runtimeName = $script:readinessRoles['runtime']
    $result = Invoke-DockerBounded @('exec',$runtimeName,'node','--input-type=module','-e',$seedResource,[string]$requestTimeout) $remaining
    $status = 0
    $normalizedError = 'docker_exec'
    if ($result.TimedOut) { $normalizedError = 'timeout' }
    elseif ($result.ExitCode -eq 0 -and $result.Output.Trim() -match '^(\d{1,3})\|([a-z_]+)$') {
        $status = [int]$Matches[1]
        $normalizedError = $Matches[2]
    }
    if ($status -ne 200) { throw (Format-ReadinessFailure 'seed' $status $normalizedError) }
    Write-Host "Readiness stage=seed status=200 elapsed_ms=$($script:readinessClock.ElapsedMilliseconds) budget_seconds=$ReadinessTimeoutSeconds"
}
try {
    Write-Host 'Building Core and Resource in Docker...'
    Build-Binary $core 'roster' './cmd/roster'
    Build-Binary $ResourceRepository 'resource-server' './cmd/roster-resource-server'
    $runtimeName = "roster-runtime-$runID"
    $resourceName = "roster-resource-$runID"
    $coreName = "roster-core-$runID"
    Start-Container $runtimeName @('--network','none','--env-file',$environment,'-e','PORT=7351','-e','ROSTER_LISTEN=127.0.0.1','-e','ROSTER_DATA_DIR=/work/runtime-data','-e','ROSTER_AGENT_MODULE=/executor/examples/runtime-agents.mjs','-v',"${RuntimeRepository}:/runtime:ro",'-v',"${core}:/executor:ro",'-v',"${work}:/work",'-w','/runtime','node:24-bookworm-slim','node','dist/main.js')
    Start-Container $resourceName @('--network',"container:$runtimeName",'--env-file',$environment,'-e','PORT=7352','-e','ROSTER_LISTEN=127.0.0.1','-e','ROSTER_DATA_DIR=/work/resource-data','-v',"${work}:/work",'node:24-bookworm-slim','/work/resource-server')
    $script:readinessRoles = @{ runtime = $runtimeName; resource = $resourceName }
    $script:readinessBudgetMilliseconds = $ReadinessTimeoutSeconds * 1000
    $script:readinessClock = [Diagnostics.Stopwatch]::StartNew()
    Wait-ReadinessStage 'runtime'
    Wait-ReadinessStage 'resource'
    Invoke-SeedOnce
    Start-ReadinessContainer $coreName @('--network',"container:$runtimeName",'--env-file',$environment,'-v',"${work}:/work",'-v',"${core}:/executor:ro",'node:24-bookworm-slim','/work/roster','serve','--config','/executor/examples/local-stack.json','--authority','/work/authority.json')
    $script:readinessRoles['core'] = $coreName
    Wait-ReadinessStage 'core'
    Invoke-Docker @('exec',$coreName,'node','/executor/scripts/verify-stack.mjs')
    $cliCheck = "import {spawnSync} from 'node:child_process';const r=spawnSync('/work/roster',['nodes.list','--file','-'],{input:'{}',encoding:'utf8'});if(r.status!==0)throw Error('CLI failed');console.log('CLI nodes:',JSON.parse(r.stdout).length);"
    Invoke-Docker @('exec',$coreName,'node','--input-type=module','-e',$cliCheck)
} finally {
    $cleanupNames = $started.ToArray()
    [array]::Reverse($cleanupNames)
    foreach ($name in $cleanupNames) { & $Docker rm -f $name | Out-Null }
    if (Test-Path -LiteralPath $environment) { Remove-Item -LiteralPath $environment }
    if (Test-Path -LiteralPath $authorityPolicy) { Remove-Item -LiteralPath $authorityPolicy }
}
