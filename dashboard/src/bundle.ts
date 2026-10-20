import { createHash } from "node:crypto";
import { lstat, readFile } from "node:fs/promises";
import { isIP } from "node:net";
import path from "node:path";

export const CLIENT_PROFILE = "roster-core-import/1";
export const RESULT_SCHEMA = "roster-import-result/1";
export const MAX_JSON_BYTES = 64 * 1024;
const MAX_CORE_RESPONSE_BYTES = 2 * 1024 * 1024;
const MAX_NODES = 20;
const MAX_SUBSCRIPTIONS = 50;
const ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$/u;
const NAME_PATTERN = /^(?:@[a-z0-9][a-z0-9._-]*\/)?[a-z0-9][a-z0-9._-]*$/u;
const VERSION_PATTERN = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*))?(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$/u;

export const PROFILE_FINGERPRINTS: Record<string, { input_schema: string; output_schema: string }> = {
  "nodes.list": { input_schema: "cd1a463c46d6264134447db17a8c3c7abe5b9a2488c6d759fea66da1f96b133e", output_schema: "99c2a23967cce546450ab9aa993878932c62fb8d0da16d984ff887f0b7d5f92b" },
  "nodes.register": { input_schema: "d8f23120d1a756ea4dad10ac8bc293a9adf3c7c5c1a6e1a1072998edbb6dedd1", output_schema: "981d2526661223ece6d735c5a0cdea473f37957b1e96c772bdbd949593d68274" },
  "subscriptions.list": { input_schema: "cd1a463c46d6264134447db17a8c3c7abe5b9a2488c6d759fea66da1f96b133e", output_schema: "d324de12229dc6e1d0865edc17a9f50cf693528c53b04fd11bb96bf9b1aa2ce5" },
  "subscriptions.create": { input_schema: "5cf83e7d0e4c735d929297d36e6064e40cdeb14597a6b98eb1590587c43e614d", output_schema: "31c748c503617892c9a9e5f9fe7ffaee999fb62cc1e12c81be667f45bae2de4b" },
};

export interface BundleNode { id: string; connector_id: string; binding_ref: string }
export interface BundleSubscription {
  id: string; source: { namespace: string; id: string }; event: string;
  target: { namespace: string; id: string }; operation: string;
  input: { values?: Record<string, unknown>; pointers?: Record<string, string> };
}
export interface BundleDocument { schema: string; name: string; version: string; client_profile: string; nodes: BundleNode[]; subscriptions: BundleSubscription[] }
export interface ResolvedBinding { url: string; target_id: string; token_env: string; watch?: boolean }
export interface BundlePlan {
  package: { name: string; version: string };
  manifest: BundleDocument;
  bindings: Record<string, ResolvedBinding>;
  actions: Array<{ command: "nodes.register" | "subscriptions.create"; id: string; input: Record<string, unknown> }>;
  digest: string;
}
export interface ImportResult {
  schema: typeof RESULT_SCHEMA; status: "planned" | "applied" | "partial" | "failed";
  package: { name: string; version: string }; client_profile: string; digest: string | null;
  actions: Array<{ command: string; id: string }>;
  applied: Array<{ command: string; id: string }>;
  unresolved: Array<{ command: string; id: string }>;
  failure: { stage: string; code: string; id?: string } | null;
}

export class BundleError extends Error {
  constructor(readonly stage: string, readonly code: string, readonly id?: string, readonly exitCode = 2) { super(code); }
}

export function parseStrictJson(text: string): unknown {
  let at = 0;
  const whitespace = () => { while (/[\u0009\u000a\u000d\u0020]/u.test(text[at] ?? "\0")) at++; };
  const fail = (): never => { throw new BundleError("input", "json_invalid"); };
  const stringValue = (): string => {
    const start = at;
    if (text[at++] !== '"') return fail();
    while (at < text.length) {
      const code = text.charCodeAt(at++);
      if (code === 0x22) {
        try { return JSON.parse(text.slice(start, at)) as string; } catch { return fail(); }
      }
      if (code < 0x20) return fail();
      if (code === 0x5c) {
        const escape = text[at++];
        if (escape === "u") { if (!/^[0-9a-fA-F]{4}$/u.test(text.slice(at, at + 4))) return fail(); at += 4; }
        else if (!escape || !'"\\/bfnrt'.includes(escape)) return fail();
      }
    }
    return fail();
  };
  const value = (depth: number): unknown => {
    if (depth > 80) return fail();
    whitespace();
    const char = text[at];
    if (char === '"') return stringValue();
    if (char === "{") {
      at++; whitespace(); const object: Record<string, unknown> = Object.create(null) as Record<string, unknown>; const keys = new Set<string>();
      if (text[at] === "}") { at++; return object; }
      while (true) {
        whitespace(); if (text[at] !== '"') return fail();
        const key = stringValue(); if (keys.has(key)) return fail(); keys.add(key);
        whitespace(); if (text[at++] !== ":") return fail(); object[key] = value(depth + 1); whitespace();
        if (text[at] === "}") { at++; return object; }
        if (text[at++] !== ",") return fail();
      }
    }
    if (char === "[") {
      at++; whitespace(); const array: unknown[] = [];
      if (text[at] === "]") { at++; return array; }
      while (true) { array.push(value(depth + 1)); whitespace(); if (text[at] === "]") { at++; return array; } if (text[at++] !== ",") return fail(); }
    }
    const rest = text.slice(at);
    for (const [literal, result] of [["true", true], ["false", false], ["null", null]] as const) {
      if (rest.startsWith(literal)) { at += literal.length; return result; }
    }
    const number = /^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/u.exec(rest)?.[0];
    if (number) { at += number.length; const parsed = Number(number); if (!Number.isFinite(parsed)) return fail(); return parsed; }
    return fail();
  };
  const parsed = value(0); whitespace(); if (at !== text.length) return fail(); return parsed;
}

function record(value: unknown, stage: string, code: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new BundleError(stage, code);
  return value as Record<string, unknown>;
}
function exactKeys(value: Record<string, unknown>, required: string[], optional: string[] = [], stage = "input"): void {
  const allowed = new Set([...required, ...optional]);
  if (Object.keys(value).some((key) => !allowed.has(key)) || required.some((key) => !(key in value))) throw new BundleError(stage, "manifest_invalid");
}
function isId(value: unknown): value is string { return typeof value === "string" && ID_PATTERN.test(value); }
function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value && typeof value === "object") {
    const row = value as Record<string, unknown>;
    return `{${Object.keys(row).sort().map((key) => `${JSON.stringify(key)}:${canonical(row[key])}`).join(",")}}`;
  }
  const json = JSON.stringify(value);
  if (json === undefined) throw new BundleError("input", "json_invalid");
  return json;
}
function digest(value: unknown): string { return createHash("sha256").update(canonical(value), "utf8").digest("hex"); }

function parseOrigin(value: unknown, mode: "core" | "provider"): string {
  if (typeof value !== "string" || value.length > 2048) throw new BundleError("bindings", "binding_invalid");
  let url: URL;
  try { url = new URL(value); } catch { throw new BundleError(mode === "core" ? "arguments" : "bindings", mode === "core" ? "core_origin_invalid" : "binding_invalid"); }
  const host = url.hostname.toLowerCase();
  const loopback = host === "localhost" || (isIP(host.replace(/^\[|\]$/gu, "")) !== 0 && (() => {
    const ip = host.replace(/^\[|\]$/gu, "");
    return isIP(ip) === 4 ? ip.startsWith("127.") : ip === "::1";
  })());
  if (url.username || url.password || url.pathname !== "/" || url.search || url.hash || !url.port && value.endsWith(":")) throw new BundleError(mode === "core" ? "arguments" : "bindings", mode === "core" ? "core_origin_invalid" : "binding_invalid");
  const coreOk = mode === "core" && url.protocol === "http:" && loopback;
  const providerOk = mode === "provider" && ((url.protocol === "http:" && loopback) || (url.protocol === "https:" && !url.username && !url.password));
  if (!coreOk && !providerOk) throw new BundleError(mode === "core" ? "arguments" : "bindings", mode === "core" ? "core_origin_invalid" : "binding_invalid");
  return url.origin;
}

function assertOrdinaryPath(target: string): Promise<void> {
  const absolute = path.resolve(target); const parsed = path.parse(absolute); const parts = absolute.slice(parsed.root.length).split(path.sep).filter(Boolean);
  let current = parsed.root;
  return (async () => {
    for (const part of parts) {
      current = path.join(current, part);
      const info = await lstat(current).catch(() => { throw new BundleError("input", "path_invalid"); });
      if (info.isSymbolicLink() || (info.mode & 0o170000) === 0o120000 || (process.platform === "win32" && (info as typeof info & { isReparsePoint?: () => boolean }).isReparsePoint?.())) throw new BundleError("input", "path_invalid");
    }
  })();
}
async function readJsonFile(target: string, maximum: number): Promise<unknown> {
  await assertOrdinaryPath(target);
  const info = await lstat(target);
  if (!info.isFile() || info.size > maximum) throw new BundleError("input", "path_invalid");
  let text: string;
  try { text = new TextDecoder("utf-8", { fatal: true }).decode(await readFile(target)); } catch { throw new BundleError("input", "file_invalid", undefined, 4); }
  if (Buffer.byteLength(text, "utf8") > maximum) throw new BundleError("input", "size_limit");
  try { return parseStrictJson(text); } catch (error) { if (error instanceof BundleError) throw error; throw new BundleError("input", "json_invalid"); }
}

function scanNoCredentials(value: unknown, depth = 0): boolean {
  if (depth > 30) return true;
  if (Array.isArray(value)) return value.some((item) => scanNoCredentials(item, depth + 1));
  if (!value || typeof value !== "object") return false;
  for (const [key, child] of Object.entries(value)) {
    if (/(?:token|secret|password|credential|authorization|api[_-]?key)/iu.test(key) || scanNoCredentials(child, depth + 1)) return true;
  }
  return false;
}

export function validateManifest(packageJsonValue: unknown, manifestValue: unknown, bindingsValue: unknown): { packageInfo: { name: string; version: string }; manifest: BundleDocument; bindings: Record<string, ResolvedBinding> } {
  const packageJson = record(packageJsonValue, "input", "package_invalid");
  if (typeof packageJson.name !== "string" || packageJson.name.length > 214 || !NAME_PATTERN.test(packageJson.name) || typeof packageJson.version !== "string" || !VERSION_PATTERN.test(packageJson.version)) throw new BundleError("input", "package_invalid");
  const packageInfo = { name: packageJson.name as string, version: packageJson.version as string };
  const roster = record(packageJson.roster, "input", "package_invalid");
  exactKeys(roster, ["manifest"], [], "input");
  if (roster.manifest !== "roster-bundle.json") throw new BundleError("input", "path_invalid");
  const raw = record(manifestValue, "manifest", "manifest_invalid");
  exactKeys(raw, ["schema", "name", "version", "client_profile", "nodes", "subscriptions"], [], "manifest");
  if (raw.schema !== "roster-bundle/1" || raw.name !== packageInfo.name || raw.version !== packageInfo.version || raw.client_profile !== CLIENT_PROFILE || !Array.isArray(raw.nodes) || !Array.isArray(raw.subscriptions) || raw.nodes.length > MAX_NODES || raw.subscriptions.length > MAX_SUBSCRIPTIONS) throw new BundleError("manifest", raw.client_profile === CLIENT_PROFILE ? "manifest_invalid" : "profile_unsupported");
  const ids = new Set<string>();
  const nodes = raw.nodes.map((entry) => {
    const row = record(entry, "manifest", "manifest_invalid"); exactKeys(row, ["id", "connector_id", "binding_ref"], [], "manifest");
    if (!isId(row.id) || !isId(row.connector_id) || !isId(row.binding_ref) || ids.has(row.id)) throw new BundleError("manifest", "manifest_invalid");
    ids.add(row.id); return { id: row.id, connector_id: row.connector_id, binding_ref: row.binding_ref };
  });
  const subscriptions = raw.subscriptions.map((entry) => {
    const row = record(entry, "manifest", "manifest_invalid"); exactKeys(row, ["id", "source", "event", "target", "operation", "input"], [], "manifest");
    if (!isId(row.id) || !isId(row.event) || !isId(row.operation) || ids.has(row.id)) throw new BundleError("manifest", "manifest_invalid");
    ids.add(row.id);
    const source = record(row.source, "manifest", "manifest_invalid"); const target = record(row.target, "manifest", "manifest_invalid");
    exactKeys(source, ["namespace", "id"], [], "manifest"); exactKeys(target, ["namespace", "id"], [], "manifest");
    if (source.namespace !== "nodes" || target.namespace !== "nodes" || !isId(source.id) || !isId(target.id) || !nodes.some((node) => node.id === source.id) || !nodes.some((node) => node.id === target.id)) throw new BundleError("manifest", "reference_invalid");
    const input = record(row.input, "manifest", "manifest_invalid"); exactKeys(input, [], ["values", "pointers"], "manifest");
    if (input.values !== undefined && (!input.values || typeof input.values !== "object" || Array.isArray(input.values) || scanNoCredentials(input.values))) throw new BundleError("manifest", "manifest_invalid");
    if (input.pointers !== undefined && (!input.pointers || typeof input.pointers !== "object" || Array.isArray(input.pointers) || Object.entries(input.pointers).some(([key, value]) => !key || typeof value !== "string" || value.length > 2048))) throw new BundleError("manifest", "manifest_invalid");
    return { id: row.id, source: { namespace: "nodes", id: source.id }, event: row.event, target: { namespace: "nodes", id: target.id }, operation: row.operation, input: input as BundleSubscription["input"] };
  });
  const bindingsRaw = record(bindingsValue, "bindings", "bindings_invalid"); exactKeys(bindingsRaw, ["bindings"], [], "bindings");
  const sourceBindings = record(bindingsRaw.bindings, "bindings", "bindings_invalid");
  if (Object.keys(sourceBindings).some((key) => !isId(key))) throw new BundleError("bindings", "bindings_invalid");
  const bindings: Record<string, ResolvedBinding> = Object.create(null) as Record<string, ResolvedBinding>;
  for (const node of nodes) {
    const value = sourceBindings[node.binding_ref];
    if (value === undefined) throw new BundleError("bindings", "binding_missing", node.id);
    const binding = record(value, "bindings", "bindings_invalid"); exactKeys(binding, ["url", "target_id", "token_env"], ["watch"], "bindings");
    const url = parseOrigin(binding.url, "provider");
    if (!isId(binding.target_id) || typeof binding.token_env !== "string" || !/^[A-Za-z_][A-Za-z0-9_]{0,127}$/u.test(binding.token_env) || (binding.watch !== undefined && typeof binding.watch !== "boolean")) throw new BundleError("bindings", "binding_invalid", node.id);
    bindings[node.binding_ref] = { url, target_id: binding.target_id, token_env: binding.token_env, ...(binding.watch === undefined ? {} : { watch: binding.watch }) };
  }
  if (Object.keys(sourceBindings).some((key) => !nodes.some((node) => node.binding_ref === key))) throw new BundleError("bindings", "binding_unused");
  const manifest: BundleDocument = { schema: "roster-bundle/1", name: packageInfo.name, version: packageInfo.version, client_profile: CLIENT_PROFILE, nodes, subscriptions };
  return { packageInfo, manifest, bindings };
}

export function coreOrigin(value: string): string { return parseOrigin(value, "core"); }

async function readResponse(response: Response): Promise<unknown> {
  if (response.status >= 300 && response.status < 400) {
    if (response.body) void response.body.cancel().catch(() => {});
    throw new BundleError("core", "redirect_forbidden", undefined, 4);
  }
  if (!response.ok) {
    if (response.body) void response.body.cancel().catch(() => {});
    throw new BundleError("core", response.status >= 500 ? "core_unavailable" : "core_rejected", undefined, 4);
  }
  if (!response.body) throw new BundleError("core", "core_response_invalid", undefined, 4);
  const reader = response.body.getReader(); const chunks: Uint8Array[] = []; let size = 0;
  try {
    while (true) {
      const part = await reader.read(); if (part.done) break;
      size += part.value.byteLength; if (size > MAX_CORE_RESPONSE_BYTES) throw new BundleError("core", "core_response_invalid", undefined, 4);
      chunks.push(part.value);
    }
  } catch (error) {
    void reader.cancel().catch(() => {});
    throw error;
  } finally { reader.releaseLock(); }
  try { return parseStrictJson(new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(chunks))); }
  catch { throw new BundleError("core", "core_response_invalid", undefined, 4); }
}

async function requestCore(origin: string, route: string, body: unknown | undefined, deadline: number, onSend?: () => void): Promise<unknown> {
  const remaining = deadline - Date.now();
  if (remaining <= 0) throw new BundleError("core", "overall_timeout", undefined, 4);
  const controller = new AbortController(); const timer = setTimeout(() => controller.abort(), Math.min(5_000, remaining));
  try {
    onSend?.();
    const response = await fetch(origin + route, { method: body === undefined ? "GET" : "POST", redirect: "manual", signal: controller.signal,
      headers: body === undefined ? { accept: "application/json" } : { accept: "application/json", "content-type": "application/json" },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }) });
    return await readResponse(response);
  } catch (error) {
    if (error instanceof BundleError) throw error;
    throw new BundleError("core", controller.signal.aborted ? "request_timeout" : "core_unavailable", undefined, 4);
  } finally { clearTimeout(timer); }
}

function fingerprint(value: unknown): string { return digest(value); }
export function checkCatalog(value: unknown): void {
  if (!Array.isArray(value)) throw new BundleError("compatibility", "catalog_incompatible");
  for (const [name, expected] of Object.entries(PROFILE_FINGERPRINTS)) {
    const matches = value.filter((entry) => record(entry, "compatibility", "catalog_incompatible").name === name);
    if (matches.length !== 1) throw new BundleError("compatibility", "catalog_incompatible");
    const row = record(matches[0], "compatibility", "catalog_incompatible");
    if (!row.input_schema || !row.output_schema || fingerprint(row.input_schema) !== expected.input_schema || fingerprint(row.output_schema) !== expected.output_schema) throw new BundleError("compatibility", "catalog_incompatible");
  }
}

function existingIds(value: unknown, kind: "nodes" | "subscriptions"): Set<string> {
  if (!Array.isArray(value)) throw new BundleError("preflight", "core_response_invalid", undefined, 4);
  const result = new Set<string>();
  for (const item of value) {
    const row = record(item, "preflight", "core_response_invalid");
    const entity = kind === "subscriptions" && row.value && typeof row.value === "object" ? record(row.value, "preflight", "core_response_invalid") : row;
    if (!isId(entity.id)) throw new BundleError("preflight", "core_response_invalid", undefined, 4);
    result.add(entity.id);
  }
  return result;
}

export async function prepareBundle(packagePath: string, bindingsPath: string, coreValue: string, deadline: number): Promise<BundlePlan> {
  const core = coreOrigin(coreValue);
  const root = path.resolve(packagePath);
  await assertOrdinaryPath(root);
  const rootInfo = await lstat(root);
  if (!rootInfo.isDirectory()) throw new BundleError("input", "path_invalid");
  const pkg = await readJsonFile(path.join(root, "package.json"), MAX_JSON_BYTES);
  const roster = record(record(pkg, "input", "package_invalid").roster, "input", "package_invalid");
  if (roster.manifest !== "roster-bundle.json") throw new BundleError("input", "path_invalid");
  const manifestPath = path.join(root, "roster-bundle.json");
  const manifestInfo = await lstat(manifestPath).catch(() => { throw new BundleError("input", "path_invalid"); });
  if (!manifestInfo.isFile()) throw new BundleError("input", "path_invalid");
  const manifest = await readJsonFile(manifestPath, MAX_JSON_BYTES);
  const bindings = await readJsonFile(path.resolve(bindingsPath), MAX_JSON_BYTES);
  const normalized = validateManifest(pkg, manifest, bindings);
  const [catalog, nodes, subscriptions] = await Promise.all([
    requestCore(core, "/api/catalog", undefined, deadline),
    requestCore(core, "/api/commands/nodes.list", {}, deadline),
    requestCore(core, "/api/commands/subscriptions.list", {}, deadline),
  ]);
  checkCatalog(catalog);
  const existingNodes = existingIds(nodes, "nodes"); const existingSubscriptions = existingIds(subscriptions, "subscriptions");
  for (const node of normalized.manifest.nodes) if (existingNodes.has(node.id)) throw new BundleError("preflight", "id_conflict", node.id);
  for (const subscription of normalized.manifest.subscriptions) if (existingSubscriptions.has(subscription.id)) throw new BundleError("preflight", "id_conflict", subscription.id);
  const actions: BundlePlan["actions"] = [
    ...normalized.manifest.nodes.map((node) => ({ command: "nodes.register" as const, id: node.id, input: {
      id: node.id, connector_id: node.connector_id, binding: normalized.bindings[node.binding_ref],
    } })),
    ...normalized.manifest.subscriptions.map((subscription) => ({ command: "subscriptions.create" as const, id: subscription.id, input: subscription as unknown as Record<string, unknown> })),
  ];
  const inputDigest = digest({ package: normalized.packageInfo, manifest: normalized.manifest, bindings: normalized.bindings, client_profile: CLIENT_PROFILE, profile_fingerprints: PROFILE_FINGERPRINTS });
  return { package: normalized.packageInfo, manifest: normalized.manifest, bindings: normalized.bindings, actions, digest: inputDigest };
}

export async function readPackageIdentity(packagePath: string): Promise<{ name: string; version: string }> {
  const root = path.resolve(packagePath);
  await assertOrdinaryPath(root);
  const info = await lstat(root);
  if (!info.isDirectory()) throw new BundleError("input", "path_invalid");
  const value = record(await readJsonFile(path.join(root, "package.json"), MAX_JSON_BYTES), "input", "package_invalid");
  if (typeof value.name !== "string" || !NAME_PATTERN.test(value.name) || typeof value.version !== "string" || !VERSION_PATTERN.test(value.version)) throw new BundleError("input", "package_invalid");
  return { name: value.name, version: value.version };
}

export function planResult(plan: BundlePlan): ImportResult {
  return { schema: RESULT_SCHEMA, status: "planned", package: plan.package, client_profile: CLIENT_PROFILE, digest: plan.digest,
    actions: plan.actions.map(({ command, id }) => ({ command, id })), applied: [], unresolved: [], failure: null };
}

export function applyExitCode(result: ImportResult): number {
  return result.status === "planned" || result.status === "applied" ? 0 : result.status === "partial" ? 3 : 4;
}

function confirmsAction(action: BundlePlan["actions"][number], value: unknown): boolean {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const response = value as Record<string, unknown>;
  if (action.command === "nodes.register") {
    return response.id === action.id && response.connector_id === action.input.connector_id;
  }
  const subject = response.subject;
  const snapshotValue = response.value;
  return !!subject && typeof subject === "object" && !Array.isArray(subject)
    && (subject as Record<string, unknown>).namespace === "subscriptions"
    && (subject as Record<string, unknown>).id === action.id
    && !!snapshotValue && typeof snapshotValue === "object" && !Array.isArray(snapshotValue)
    && (snapshotValue as Record<string, unknown>).id === action.id;
}

export async function applyBundle(plan: BundlePlan, coreValue: string, deadline: number): Promise<ImportResult> {
  const result = planResult(plan); result.status = "applied";
  for (const action of plan.actions) {
    let sendStarted = false;
    try {
      const response = await requestCore(coreOrigin(coreValue), `/api/commands/${action.command}`, action.input, deadline, () => { sendStarted = true; });
      if (!confirmsAction(action, response)) throw new BundleError("apply", "core_response_invalid", action.id, 4);
      result.applied.push({ command: action.command, id: action.id });
    } catch (error) {
      const failure = error instanceof BundleError ? error : new BundleError("apply", "core_unknown", action.id, 3);
      result.status = result.applied.length === 0 && !sendStarted ? "failed" : "partial";
      if (sendStarted && (failure.code === "request_timeout" || failure.code === "core_unavailable" || failure.code === "core_response_invalid" || failure.code === "overall_timeout" || failure.code === "core_unknown")) result.unresolved.push({ command: action.command, id: action.id });
      result.failure = { stage: "apply", code: failure.code, id: action.id };
      return result;
    }
  }
  return result;
}
