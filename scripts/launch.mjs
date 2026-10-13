#!/usr/bin/env node
import { randomUUID } from 'node:crypto';
import { constants as fsConstants, readdirSync, readFileSync } from 'node:fs';
import { access, chmod, lstat, mkdir, open, readFile, readdir, unlink } from 'node:fs/promises';
import { createServer } from 'node:net';
import { spawn } from 'node:child_process';
import { dirname, isAbsolute, join, parse, relative, resolve, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { performance } from 'node:perf_hooks';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const CORE_ROOT = join(ROOT, 'core');
const DASHBOARD_ROOT = join(ROOT, 'dashboard');
const RESERVED = new Set(['PATH','SYSTEMROOT','WINDIR','HOME','USERPROFILE','TMP','TEMP','NODE_OPTIONS','NODE_PATH','LD_PRELOAD','LD_LIBRARY_PATH','PORT','ROSTER_LISTEN','ROSTER_DATA_DIR']);
const TOKEN_NAME = /^[A-Za-z_][A-Za-z0-9_]{0,127}$/u;
const TOTAL_MS = 30_000;
const STARTUP_MS = 60_000;

class LaunchFailure extends Error {
  constructor(component, code, exit = 2) { super(code); this.component = component; this.code = code; this.exit = exit; }
}
function fail(component, code, exit = 2) { throw new LaunchFailure(component, code, exit); }
function jsonResult(phase, component, code, children = {}, unresolved = []) {
  process.stdout.write(`${JSON.stringify({ schema: 'roster-launch-result/1', phase, component, code, children, unresolved })}\n`);
}
function inside(parent, child) {
  const rel = relative(parent, child);
  return rel === '' || (!rel.startsWith(`..${sep}`) && rel !== '..' && !isAbsolute(rel));
}
function overlap(a, b) { return inside(a, b) || inside(b, a); }
function resolveInputPath(value, base) {
  if (typeof value !== 'string' || !value.trim()) fail('preflight', 'path_invalid');
  return resolve(base, value);
}
export function parseStrict(text) {
  let i = 0;
  const ws = () => { while (/[\t\n\r ]/u.test(text[i] ?? '\0')) i++; };
  const bad = () => fail('preflight', 'json_invalid');
  const str = () => {
    const start = i;
    if (text[i++] !== '"') return bad();
    while (i < text.length) {
      const c = text.charCodeAt(i++);
      if (c === 34) { try { return JSON.parse(text.slice(start, i)); } catch { return bad(); } }
      if (c < 32) return bad();
      if (c === 92) {
        const e = text[i++];
        if (e === 'u') { if (!/^[\da-f]{4}$/iu.test(text.slice(i, i + 4))) return bad(); i += 4; }
        else if (!e || !'"\\/bfnrt'.includes(e)) return bad();
      }
    }
    return bad();
  };
  const value = (depth = 0) => {
    if (depth > 64) return bad();
    ws(); const c = text[i];
    if (c === '"') return str();
    if (c === '{') {
      i++; ws(); const obj = Object.create(null); const keys = new Set();
      if (text[i] === '}') { i++; return obj; }
      while (true) {
        ws(); if (text[i] !== '"') return bad(); const k = str();
        if (keys.has(k)) return bad(); keys.add(k); ws(); if (text[i++] !== ':') return bad(); obj[k] = value(depth + 1); ws();
        if (text[i] === '}') { i++; return obj; } if (text[i++] !== ',') return bad();
      }
    }
    if (c === '[') {
      i++; ws(); const arr = []; if (text[i] === ']') { i++; return arr; }
      while (true) { arr.push(value(depth + 1)); ws(); if (text[i] === ']') { i++; return arr; } if (text[i++] !== ',') return bad(); }
    }
    for (const [lit, v] of [['true', true], ['false', false], ['null', null]]) if (text.startsWith(lit, i)) { i += lit.length; return v; }
    const n = /^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/u.exec(text.slice(i))?.[0];
    if (n) { i += n.length; const parsed = Number(n); if (!Number.isFinite(parsed)) return bad(); return parsed; }
    return bad();
  };
  const result = value(); ws(); if (i !== text.length) return bad(); return result;
}
function exactKeys(obj, required, optional = []) {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) fail('preflight', 'config_invalid');
  const allowed = new Set([...required, ...optional]);
  if (Object.keys(obj).some(k => !allowed.has(k)) || required.some(k => !(k in obj))) fail('preflight', 'config_invalid');
}
function strictOwnerOnly(info, directory) {
  if (process.platform === 'win32') return;
  if (typeof process.getuid === 'function' && info.uid !== process.getuid()) fail('preflight', 'owner_mismatch');
  if ((info.mode & 0o077) !== 0) fail('preflight', directory ? 'data_permissions' : 'file_permissions');
}
async function noSymlinkPath(target) {
  const absolute = resolve(target); const root = parse(absolute).root;
  let current = root;
  for (const part of absolute.slice(root.length).split(sep).filter(Boolean)) {
    current = join(current, part);
    const info = await lstat(current).catch(() => fail('preflight', 'path_missing'));
    if (info.isSymbolicLink()) fail('preflight', 'path_reparse');
  }
  return absolute;
}
async function checkExistingParents(target) {
  const absolute = resolve(target); const root = parse(absolute).root; let current = root;
  for (const part of absolute.slice(root.length).split(sep).filter(Boolean)) {
    current = join(current, part);
    let info; try { info = await lstat(current); } catch (error) { if (error.code === 'ENOENT') return; fail('preflight', 'path_invalid'); }
    if (info.isSymbolicLink()) fail('preflight', 'path_reparse');
    if (current !== absolute && !info.isDirectory()) fail('preflight', 'path_invalid');
  }
}
async function ordinaryFile(target, { executable = false, max = 64 * 1024, ownerOnly = false } = {}) {
  await noSymlinkPath(target); const info = await lstat(target).catch(() => fail('preflight', 'path_missing'));
  if (!info.isFile() || info.size > max) fail('preflight', 'file_invalid');
  if (ownerOnly) strictOwnerOnly(info, false);
  if (executable && process.platform !== 'win32') await access(target, fsConstants.X_OK).catch(() => fail('preflight', 'not_executable'));
  return info;
}
async function readJson(target, ownerOnly = true) {
  await ordinaryFile(target, { ownerOnly });
  let raw; try { raw = new TextDecoder('utf-8', { fatal: true }).decode(await readFile(target)); } catch { fail('preflight', 'file_invalid'); }
  if (Buffer.byteLength(raw, 'utf8') > 64 * 1024) fail('preflight', 'size_limit');
  return parseStrict(raw);
}
function assertOutsideRepos(pathname, repos) {
  if (repos.some(repo => overlap(repo, pathname))) fail('preflight', 'path_in_repository');
}
function parseArgs(argv) {
  if (argv.length !== 2 || argv[0] !== '--config' || !argv[1] || argv[1].startsWith('--')) fail('preflight', 'usage_invalid');
  return resolve(argv[1]);
}
function extractCoreTokenRefs(config) {
  const parsed = parseStrict(config);
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) fail('preflight', 'core_config_invalid');
  if (Object.keys(parsed).some(k => !['nodes','subscriptions','conversations'].includes(k))) fail('preflight', 'core_config_invalid');
  if (parsed.nodes !== undefined && !Array.isArray(parsed.nodes)) fail('preflight', 'core_config_invalid');
  const refs = new Set();
  for (const node of parsed.nodes ?? []) {
    if (!node || typeof node !== 'object' || Array.isArray(node)) fail('preflight', 'core_config_invalid');
    const binding = node.binding;
    if (binding && typeof binding === 'object' && !Array.isArray(binding) && typeof binding.token_env === 'string' && binding.token_env) {
      if (!TOKEN_NAME.test(binding.token_env)) fail('preflight', 'core_config_invalid');
      refs.add(binding.token_env);
    }
  }
  return refs;
}
function childBaseEnv() {
  const names = ['PATH','SystemRoot','WINDIR','HOME','USERPROFILE','HOMEDRIVE','HOMEPATH','TMP','TEMP','TMPDIR','LANG','LC_ALL','COMSPEC','PATHEXT'];
  const env = Object.create(null);
  for (const name of names) if (process.env[name] !== undefined) env[name] = process.env[name];
  return env;
}
async function portFree(port) {
  const probe = createServer();
  await new Promise((resolveListen, reject) => {
    probe.once('error', reject); probe.listen(port, '127.0.0.1', resolveListen);
  }).catch(error => {
    if (['EADDRINUSE','EACCES'].includes(error.code)) fail('preflight', 'port_unavailable');
    fail('preflight', 'port_probe_failed', 2);
  });
  await new Promise((resolveClose, reject) => probe.close(error => error ? reject(error) : resolveClose()));
}
async function prepare(configPath, started) {
  if (performance.now() - started > TOTAL_MS) fail('preflight', 'preflight_timeout', 2);
  await ordinaryFile(configPath, { ownerOnly: true });
  const config = await readJson(configPath);
  const optional = ['core_port','dashboard_port','core_config','core_token_env'];
  exactKeys(config, ['schema','data_root','env_file','core_binary','dashboard_entry'], optional);
  if (config.schema !== 'roster-launch/1') fail('preflight', 'config_invalid');
  if ((config.core_port !== undefined && config.core_port !== 7350) || (config.dashboard_port !== undefined && config.dashboard_port !== 7360)) fail('preflight', 'port_invalid');
  const base = dirname(configPath);
  const paths = {
    data: resolveInputPath(config.data_root, base), envFile: resolveInputPath(config.env_file, base),
    core: resolveInputPath(config.core_binary, base), gateway: resolveInputPath(config.dashboard_entry, base),
    coreConfig: config.core_config === undefined ? null : resolveInputPath(config.core_config, base),
  };
  for (const candidate of [configPath, paths.data, paths.envFile, paths.coreConfig].filter(Boolean)) assertOutsideRepos(candidate, [ROOT]);
  await ordinaryFile(paths.envFile, { ownerOnly: true });
  const envDoc = await readJson(paths.envFile);
  exactKeys(envDoc, ['env']);
  if (!envDoc.env || typeof envDoc.env !== 'object' || Array.isArray(envDoc.env)) fail('preflight', 'env_invalid');
  for (const [name, value] of Object.entries(envDoc.env)) if (!TOKEN_NAME.test(name) || typeof value !== 'string') fail('preflight', 'env_invalid');
  const allow = config.core_token_env ?? [];
  if (!Array.isArray(allow) || allow.length > 100 || allow.some(name => typeof name !== 'string' || !TOKEN_NAME.test(name)) || new Set(allow).size !== allow.length) fail('preflight', 'env_allowlist_invalid');
  const allowedNames = new Set(allow);
  for (const name of allowedNames) if (RESERVED.has(name.toUpperCase())) fail('preflight', 'env_allowlist_invalid');
  let coreConfigText = '';
  if (paths.coreConfig) {
    await ordinaryFile(paths.coreConfig, { ownerOnly: true });
    coreConfigText = new TextDecoder('utf-8', { fatal: true }).decode(await readFile(paths.coreConfig));
    if (Buffer.byteLength(coreConfigText, 'utf8') > 64 * 1024) fail('preflight', 'size_limit');
    for (const name of extractCoreTokenRefs(coreConfigText)) allowedNames.add(name);
  }
  const coreEnv = Object.create(null);
  for (const name of allowedNames) {
    if (RESERVED.has(name.toUpperCase())) fail('preflight', 'env_allowlist_invalid');
    const value = envDoc.env[name];
    if (value !== undefined) coreEnv[name] = value;
    else if (coreConfigText && extractCoreTokenRefs(coreConfigText).has(name)) fail('preflight', 'env_missing');
  }
  await ordinaryFile(paths.core, { executable: true, max: 2 * 1024 * 1024 * 1024 });
  const expectedGateway = join(DASHBOARD_ROOT, 'build', 'src', 'gateway.js');
  if (resolve(paths.gateway) !== expectedGateway) fail('preflight', 'dashboard_entry_invalid');
  await ordinaryFile(paths.gateway, { max: 16 * 1024 * 1024 });
  await ordinaryFile(join(DASHBOARD_ROOT, 'dist', 'index.html'), { max: 32 * 1024 * 1024 });
  const rootInfo = await lstat(paths.data).catch(() => null);
  if (rootInfo && !rootInfo.isDirectory()) fail('preflight', 'data_root_invalid');
  if (rootInfo) { await noSymlinkPath(paths.data); strictOwnerOnly(rootInfo, true); }
  await portFree(7350); await portFree(7360);
  if (performance.now() - started > TOTAL_MS) fail('preflight', 'preflight_timeout', 2);
  return { config, paths, coreEnv, started };
}
async function initDataRoot(dataRoot, role) {
  let created = false;
  await checkExistingParents(dataRoot);
  try { await mkdir(dataRoot, { mode: 0o700 }); created = true; } catch (error) { if (error.code !== 'EEXIST') fail('preflight', 'data_root_invalid'); }
  await noSymlinkPath(dataRoot);
  let info = await lstat(dataRoot); if (!info.isDirectory()) fail('preflight', 'data_root_invalid');
  if (created && process.platform !== 'win32') await chmod(dataRoot, 0o700);
  info = await lstat(dataRoot); strictOwnerOnly(info, true);
  const marker = join(dataRoot, '.roster-launcher.json');
  const existing = await lstat(marker).catch(() => null);
  if (existing) {
    if (!existing.isFile() || existing.isSymbolicLink()) fail('preflight', 'marker_invalid');
    const value = await readJson(marker);
    exactKeys(value, ['schema','role']);
    if (value.schema !== 'roster-launch/1' || value.role !== role) fail('preflight', 'marker_mismatch');
  } else {
    const entries = await readdir(dataRoot);
    if (entries.length !== 0) fail('preflight', 'data_root_unmanaged');
    await writeOwnerFile(marker, JSON.stringify({ schema: 'roster-launch/1', role }));
  }
  return marker;
}
async function writeOwnerFile(target, text) {
  const handle = await open(target, 'wx', 0o600).catch(() => fail('preflight', 'lock_conflict'));
  try { await handle.writeFile(text, 'utf8'); await handle.sync(); } finally { await handle.close(); }
}
async function acquireLock(dataRoot) {
  const path = join(dataRoot, 'launcher.lock'); const token = randomUUID();
  await writeOwnerFile(path, JSON.stringify({ run_id: token, pid: process.pid }));
  return { path, token };
}
async function releaseLock(lock) {
  if (!lock) return true;
  try {
    const info = await lstat(lock.path);
    if (!info.isFile() || info.isSymbolicLink() || info.size > 1024) return false;
    const value = parseStrict(await readFile(lock.path, 'utf8'));
    exactKeys(value, ['run_id','pid']);
    if (value.run_id !== lock.token || value.pid !== process.pid) return false;
    await unlink(lock.path); return true;
  } catch { return false; }
}
function spawnOwned(name, executable, args, cwd, env) {
  const child = spawn(executable, args, { cwd, env, stdio: 'ignore', detached: process.platform !== 'win32', windowsHide: true });
  const rec = { name, child, pid: null, groupOwned: process.platform !== 'win32', code: null, signal: null, exited: false, spawnError: false, exitPromise: null };
  rec.exitPromise = new Promise(resolveExit => {
    child.once('error', () => { rec.spawnError = true; rec.exited = true; resolveExit(rec); });
    child.once('exit', (code, signal) => { rec.code = code; rec.signal = signal; rec.exited = true; resolveExit(rec); });
  });
  child.once('spawn', () => { rec.pid = child.pid ?? null; });
  return rec;
}
function signalChild(rec, signal) {
  if (!rec.child.pid) return;
  try {
    if (rec.groupOwned) process.kill(-rec.pid, signal);
    else if (!rec.exited) rec.child.kill(signal);
  } catch (error) { if (error.code !== 'ESRCH') throw error; }
}
function wait(ms) { return new Promise(resolveWait => setTimeout(resolveWait, ms)); }
function groupAlive(child) {
  if (!child.groupOwned || !child.pid) return false;
  try { process.kill(-child.pid, 0); } catch (error) { return error.code !== 'ESRCH'; }
  if (process.platform !== 'linux') return true;
  try {
    for (const entry of readdirSync('/proc')) {
      if (!/^\d+$/u.test(entry)) continue;
      try {
        const stat = readFileSync(`/proc/${entry}/stat`, 'utf8');
        const fields = stat.slice(stat.lastIndexOf(')') + 2).trim().split(/\s+/u);
        if (fields[2] === String(child.pid) && fields[0] !== 'Z' && fields[0] !== 'X') return true;
      } catch { /* process exited while the bounded proc snapshot was read */ }
    }
    return false;
  } catch { return true; }
}
async function waitAll(children, ms) {
  const deadline = performance.now() + ms;
  while (true) {
    if (children.every(c => c.exited && !groupAlive(c))) return true;
    const remaining = deadline - performance.now(); if (remaining <= 0) return false;
    await wait(Math.min(50, remaining));
  }
}
async function terminate(children, graceMs, forceMs) {
  for (const child of children) signalChild(child, 'SIGTERM');
  let graceful = await waitAll(children, graceMs);
  if (!graceful) {
    for (const child of children) if (!child.exited || child.groupOwned) signalChild(child, 'SIGKILL');
    await waitAll(children, forceMs);
  }
  return { forced: !graceful, unresolved: children.filter(c => !c.exited || groupAlive(c)).map(c => c.name) };
}
async function jsonHttp(url, options = {}, timeoutMs = 2_000) {
  const controller = new AbortController(); const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { ...options, redirect: 'manual', signal: controller.signal });
    if (!response.ok) { if (response.body) void response.body.cancel().catch(() => {}); return null; }
    if (!response.body) return null;
    const reader = response.body.getReader(); const chunks = []; let size = 0;
    try {
      while (true) { const part = await reader.read(); if (part.done) break; size += part.value.length; if (size > 64 * 1024) { void reader.cancel().catch(() => {}); return null; } chunks.push(part.value); }
    } finally { reader.releaseLock(); }
    return parseStrict(new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(chunks)));
  } catch { return null; } finally { clearTimeout(timer); }
}
async function ready(children, deadline) {
  while (performance.now() < deadline) {
    const exited = children.find(c => c.exited);
    if (exited) fail(exited.name, 'child_exited', 3);
    const timeout = Math.max(1, Math.min(2_000, deadline - performance.now()));
    const core = await jsonHttp('http://127.0.0.1:7350/ready', {}, timeout);
    if (core?.ready === true && typeof core.instance_id === 'string' && core.instance_id.length > 0) {
      const remain = deadline - performance.now();
      if (remain <= 0) break;
      const overview = await jsonHttp('http://127.0.0.1:7360/api/overview', { headers: { 'sec-fetch-site': 'none' } }, Math.max(1, Math.min(2_000, remain)));
      if (overview?.core?.instance_id === core.instance_id && overview?.sources?.core?.error === null && overview?.meta?.stale === false) return;
    }
    await wait(150);
  }
  fail('launcher', 'readiness_timeout', 3);
}
function childSummary(children) { return Object.fromEntries(children.map(c => [c.name, c.code])); }
function serverShutdownOkay(children, forced, unresolved) {
  if (forced || unresolved.length) return false;
  const core = children.find(c => c.name === 'core'); const gateway = children.find(c => c.name === 'gateway');
  const coreOkay = core?.code === 0;
  const gatewayOkay = gateway?.code === 0 || gateway?.signal === 'SIGTERM';
  return coreOkay && gatewayOkay;
}

export async function launch(argv = process.argv.slice(2)) {
  const started = performance.now(); let lock = null; let children = []; let stopping = false; let signalName = null; let readyWritten = false;
  let signalResolve;
  const signalPromise = new Promise(resolveSignal => { signalResolve = resolveSignal; });
  const onInt = () => { if (!signalName) { signalName = 'SIGINT'; signalResolve(signalName); } };
  const onTerm = () => { if (!signalName) { signalName = 'SIGTERM'; signalResolve(signalName); } };
  process.on('SIGINT', onInt); process.on('SIGTERM', onTerm);
  try {
    const configPath = parseArgs(argv);
    const prepared = await prepare(configPath, started);
    await initDataRoot(prepared.paths.data, 'server');
    lock = await acquireLock(prepared.paths.data);
    if (prepared.config.core_config) {
      const coreConfig = resolveInputPath(prepared.config.core_config, dirname(configPath));
      children.push(spawnOwned('core', prepared.paths.core, ['serve','--listen','127.0.0.1:7350','--config',coreConfig], CORE_ROOT,
        { ...childBaseEnv(), ...prepared.coreEnv }));
    } else {
      children.push(spawnOwned('core', prepared.paths.core, ['serve','--listen','127.0.0.1:7350'], CORE_ROOT,
        { ...childBaseEnv(), ...prepared.coreEnv }));
    }
    children.push(spawnOwned('gateway', process.execPath, [prepared.paths.gateway], DASHBOARD_ROOT, { ...childBaseEnv(), PORT: '7360' }));
    const startupDeadline = performance.now() + STARTUP_MS;
    const readyTask = ready(children, startupDeadline);
    await Promise.race([readyTask, signalPromise.then(() => fail('launcher', 'startup_interrupted', 3))]);
    if (performance.now() > startupDeadline) fail('launcher', 'readiness_timeout', 3);
    jsonResult('ready', 'launcher', 0); readyWritten = true;
    const firstExit = Promise.race(children.map(c => c.exitPromise));
    const event = await Promise.race([signalPromise.then(signal => ({ type: 'signal', signal })), firstExit.then(child => ({ type: 'child', child }))]);
    stopping = true;
    if (event.type === 'child') {
      const shutdown = await terminate(children, 8_000, 2_000);
      const unresolved = [...shutdown.unresolved];
      if (unresolved.length === 0) { if (!await releaseLock(lock)) unresolved.push('lock'); else lock = null; }
      jsonResult('failed', event.child.name, 3, childSummary(children), unresolved);
      return 3;
    }
    const shutdown = await terminate(children, 8_000, 2_000);
    const okay = serverShutdownOkay(children, shutdown.forced, shutdown.unresolved);
    const unresolved = [...shutdown.unresolved];
    if (unresolved.length === 0) { if (!await releaseLock(lock)) unresolved.push('lock'); else lock = null; }
    const code = okay && unresolved.length === 0 ? 0 : 3;
    jsonResult(code === 0 ? 'stopped' : 'failed', 'launcher', code, childSummary(children), unresolved);
    return code;
  } catch (error) {
    const failure = error instanceof LaunchFailure ? error : new LaunchFailure('preflight', 'local_io_failure', 2);
    stopping = true;
    let unresolved = [];
    if (children.length) {
      const shutdown = await terminate(children, 8_000, 2_000);
      unresolved = shutdown.unresolved;
    }
    if (unresolved.length === 0 && lock) { if (await releaseLock(lock)) lock = null; else unresolved.push('lock'); }
    const exit = children.length ? 3 : failure.exit;
    jsonResult('failed', failure.component, exit, childSummary(children), unresolved);
    return exit;
  } finally {
    process.removeListener('SIGINT', onInt); process.removeListener('SIGTERM', onTerm);
    if (!readyWritten && lock && children.length === 0) await releaseLock(lock);
    void stopping;
  }
}

if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url)
  void launch().then(code => { process.exitCode = code; }).catch(() => { process.exitCode = 3; });
