param(
    [string]$Docker = 'docker',
    [string]$WorkDirectory = (Join-Path ([IO.Path]::GetTempPath()) 'roster-integration-tests'),
    [string]$CacheDirectory = '',
    [string]$RuntimeRepository,
    [string]$ResourceRepository
)
$ErrorActionPreference = 'Stop'
$core = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$serverRoot = Split-Path $core -Parent
$workspaceRoot = Split-Path $serverRoot -Parent
$clientRoot = Join-Path $workspaceRoot 'roster-node-client'
if (-not $RuntimeRepository) { $RuntimeRepository = Join-Path $clientRoot 'runtime' }
if (-not $ResourceRepository) { $ResourceRepository = Join-Path $clientRoot 'resource' }
if (-not (Test-Path (Join-Path $RuntimeRepository 'dist/main.js'))) { throw 'Build roster-node-client/runtime first (dist and node_modules must exist).' }
$runID = [guid]::NewGuid().ToString('N').Substring(0,8)
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
    $arguments += @('golang:1.26','go','build','-o',"/out/$Output",$Package)
    Invoke-Docker $arguments | Out-Null
}
function Start-Container([string]$Name,[string[]]$Arguments) {
    Invoke-Docker (@('run','-d','--name',$Name) + $Arguments) | Out-Null
    $started.Add($Name)
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
    $seed = @'
const delay=ms=>new Promise(r=>setTimeout(r,ms));
for(let i=0;;i++){try{const r=await fetch('http://127.0.0.1:7351/state',{headers:{Authorization:'Bearer '+process.env.ROSTER_TOKEN}});if(!r.ok)throw Error();const s=await fetch('http://127.0.0.1:7352/v1/resources',{method:'POST',headers:{Authorization:'Bearer '+process.env.ROSTER_RESOURCE_ADMIN_TOKEN,'Content-Type':'application/json'},body:JSON.stringify({id:'notes',name:'Notes',component:'document',version:0})});if(!s.ok)throw Error();break;}catch(e){if(i===59)throw Error('Existing servers did not become ready');await delay(250);}}
'@
    Invoke-Docker @('exec',$runtimeName,'node','--input-type=module','-e',$seed) | Out-Null
    Start-Container $coreName @('--network',"container:$runtimeName",'--env-file',$environment,'-v',"${work}:/work",'-v',"${core}:/executor:ro",'node:24-bookworm-slim','/work/roster','serve','--config','/executor/examples/local-stack.json','--authority','/work/authority.json')
    $ready = "for(let i=0;;i++){try{const r=await fetch('http://127.0.0.1:7350/health');if(r.ok)break;}catch{}if(i===59)throw Error('Core did not become ready');await new Promise(r=>setTimeout(r,250));}"
    Invoke-Docker @('exec',$coreName,'node','--input-type=module','-e',$ready) | Out-Null
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
