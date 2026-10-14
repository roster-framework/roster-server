import { createHash } from "node:crypto";
import { lstat, readFile } from "node:fs/promises";
import { isIP } from "node:net";
import path from "node:path";
import { applyBundle, applyExitCode, BundleError, coreOrigin, parseStrictJson, planResult, prepareBundle, type BundlePlan } from "./bundle.js";

const DEPLOYMENT_SCHEMA = "roster-deployment/1";
const RESULT_SCHEMA = "roster-deployment-result/1";
const ID = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$/u;
const TOKEN_ENV = /^[A-Z][A-Z0-9_]{0,99}$/u;
const AGENT_FIELDS = new Set(["tool_approval", "name", "description", "kind", "engine", "model_parameters", "provider", "model", "credential_env", "base_url", "statuses", "events", "rules"]);
const DEFAULT_BASE_URL = "https://api.openai.com/v1";
const MODEL_PARAMETER_RESERVED = /api.?key|credential|authorization|header|(?:base|server|api).*url|endpoint|client|callback|configurable|(?:access|auth|session|bearer).*token|^(?:token|configuration|model|model.?provider|provider|engine|tools?|messages?|prompt|response.?format|custom_openai|__proto__|prototype|constructor)$/iu;
const AGENT_ENGINE = new Set(["langgraph-node", "langgraph-python", "crewai-python"]);
const AGENT_FIELDS_COMPLETE = new Set(["id", "name", "description", "kind", "version", "engine", "model_parameters", "provider", "instructions", "model", "credential_env", "base_url", "statuses", "events", "mcp_ids", "skills", "rules", "tool_approval"]);

type JsonRecord = Record<string, unknown>;
type Ref = { id: string; origin: string; token_env: string; expected_version: number; node_id: string };
type AgentDecl = { id: string; expected_version: number; profile_ref: string; config: JsonRecord; prompt_ref?: string; skill_refs: string[]; mcp_refs: string[]; resource_refs: string[] };
type Deployment = { schema: string; id: string; runtime: { origin: string; token_env: string }; core: { origin: string }; profiles: Record<string, JsonRecord>; agents: AgentDecl[]; resources: Ref[]; bundle: { package_dir: string; bindings_file: string } };
type AgentPlan = { id: string; version: number; payload: JsonRecord; prompt_hash: string | null };
type Preflight = { declaration: Deployment; runtimeOrigin: string; runtimeId: string; coreOrigin: string; runtimeToken: string; resourceTokens: Map<string, string>; agents: AgentPlan[]; resources: Ref[]; bundle: BundlePlan; digest: string };
type Entry = { service: string; id: string; revision: number | null };
type Unresolved = { service: string; id: string };
type Result = { schema: string; deployment_id: string; status: "planned" | "applied" | "partial" | "failed"; digest: string | null; applied: Entry[]; unresolved: Unresolved[]; code: string };

function fail(stage: string, code: string, exitCode = 2): never { throw new BundleError(stage, code, undefined, exitCode); }
function obj(value: unknown, code = "deployment_invalid"): JsonRecord { if (!value || typeof value !== "object" || Array.isArray(value)) fail("input", code); return value as JsonRecord; }
function exact(value: JsonRecord, required: string[], optional: string[] = []): void {
  const allow = new Set([...required, ...optional]);
  if (Object.keys(value).some((key) => !allow.has(key)) || required.some((key) => !Object.hasOwn(value, key))) fail("input", "deployment_invalid");
}
function identifier(value: unknown): value is string { return typeof value === "string" && ID.test(value); }
function tokenEnv(value: unknown): value is string { return typeof value === "string" && TOKEN_ENV.test(value); }
function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value && typeof value === "object") return `{${Object.keys(value as JsonRecord).sort().map((key) => `${JSON.stringify(key)}:${canonical((value as JsonRecord)[key])}`).join(",")}}`;
  const out = JSON.stringify(value); if (out === undefined) fail("input", "deployment_invalid"); return out;
}
function sha(value: string | Uint8Array): string { return createHash("sha256").update(value).digest("hex"); }
function loopback(url: URL): boolean {
  const host = url.hostname.toLowerCase().replace(/^\[|\]$/gu, "");
  return host === "localhost" || isIP(host) === 4 && host.startsWith("127.") || host === "::1";
}
function providerOrigin(value: unknown): string {
  if (typeof value !== "string" || value.length > 2048) fail("input", "origin_invalid");
  let url: URL; try { url = new URL(value); } catch { return fail("input", "origin_invalid"); }
  if (url.username || url.password || url.pathname !== "/" || url.search || url.hash || !((url.protocol === "http:" && loopback(url)) || url.protocol === "https:")) fail("input", "origin_invalid");
  return url.origin;
}
function relativePath(value: unknown): string {
  if (typeof value !== "string" || !value || value.length > 2048 || path.isAbsolute(value) || path.win32.isAbsolute(value) || path.win32.parse(value).root || value.includes("\0")) fail("input", "path_invalid");
  return value;
}
function resolveRelative(base: string, value: string): string {
  const resolved = path.resolve(base, value); const rel = path.relative(base, resolved);
  if (!rel || rel === ".." || rel.startsWith(`..${path.sep}`) || path.isAbsolute(rel)) fail("input", "path_invalid");
  return resolved;
}
async function ordinaryPath(target: string): Promise<void> {
  const absolute = path.resolve(target); const parsed = path.parse(absolute); let current = parsed.root;
  for (const part of absolute.slice(parsed.root.length).split(path.sep).filter(Boolean)) {
    current = path.join(current, part);
    const info = await lstat(current).catch(() => fail("input", "path_invalid"));
    if (info.isSymbolicLink() || (process.platform === "win32" && (info as typeof info & { isReparsePoint?: () => boolean }).isReparsePoint?.())) fail("input", "path_invalid");
  }
}
async function readUtf8File(target: string, maximum: number): Promise<{ bytes: Buffer; text: string }> {
  await ordinaryPath(target);
  const info = await lstat(target); if (!info.isFile() || info.size > maximum) fail("input", "file_invalid");
  let bytes: Buffer; try { bytes = await readFile(target); } catch { return fail("input", "file_invalid"); }
  if (bytes.byteLength > maximum) fail("input", "size_limit");
  let text: string; try { text = new TextDecoder("utf-8", { fatal: true }).decode(bytes); } catch { return fail("input", "file_invalid"); }
  return { bytes, text };
}
function depthCheck(value: unknown, depth = 0): void {
  if (depth > 32) fail("input", "depth_limit");
  if (Array.isArray(value)) { for (const item of value) depthCheck(item, depth + 1); return; }
  if (value && typeof value === "object") for (const [key, child] of Object.entries(value)) {
    if (["__proto__", "constructor", "prototype"].includes(key)) fail("input", "deployment_invalid");
    depthCheck(child, depth + 1);
  }
}
function normalizeDeployment(value: unknown): Deployment {
  const root = obj(value); exact(root, ["schema", "id", "runtime", "core", "profiles", "agents", "resources", "bundle"]);
  if (root.schema !== DEPLOYMENT_SCHEMA || !identifier(root.id)) fail("input", "deployment_invalid");
  const runtime = obj(root.runtime); exact(runtime, ["origin", "token_env"]);
  const core = obj(root.core); exact(core, ["origin"]);
  if (!tokenEnv(runtime.token_env)) fail("input", "token_env_invalid");
  const profilesRaw = obj(root.profiles); const profiles: Record<string, JsonRecord> = Object.create(null) as Record<string, JsonRecord>;
  for (const [id, raw] of Object.entries(profilesRaw)) {
    if (!identifier(id)) fail("input", "deployment_invalid");
    const config = obj(raw); if (Object.keys(config).some((key) => !AGENT_FIELDS.has(key))) fail("input", "agent_config_invalid"); profiles[id] = config;
  }
  if (!Array.isArray(root.agents) || root.agents.length < 1 || root.agents.length > 32) fail("input", "deployment_invalid");
  const agentIds = new Set<string>();
  const agents = root.agents.map((raw): AgentDecl => {
    const row = obj(raw); exact(row, ["id", "expected_version", "profile_ref", "config", "skill_refs", "mcp_refs", "resource_refs"], ["prompt_ref"]);
    if (!identifier(row.id) || agentIds.has(row.id) || !Number.isSafeInteger(row.expected_version) || (row.expected_version as number) < 0 || !identifier(row.profile_ref) || !Object.hasOwn(profiles, row.profile_ref)) fail("input", "agent_config_invalid");
    agentIds.add(row.id);
    const config = obj(row.config); if (Object.keys(config).some((key) => !AGENT_FIELDS.has(key))) fail("input", "agent_config_invalid");
    if (!Array.isArray(row.skill_refs) || row.skill_refs.length > 20 || row.skill_refs.some((x) => !identifier(x)) || new Set(row.skill_refs).size !== row.skill_refs.length ||
      !Array.isArray(row.mcp_refs) || row.mcp_refs.length > 1024 || row.mcp_refs.some((x) => !identifier(x)) || new Set(row.mcp_refs).size !== row.mcp_refs.length ||
      !Array.isArray(row.resource_refs) || row.resource_refs.length > 32 || row.resource_refs.some((x) => !identifier(x)) || new Set(row.resource_refs).size !== row.resource_refs.length) fail("input", "agent_refs_invalid");
    if (row.prompt_ref !== undefined) relativePath(row.prompt_ref);
    return { id: row.id, expected_version: row.expected_version as number, profile_ref: row.profile_ref, config, ...(row.prompt_ref === undefined ? {} : { prompt_ref: row.prompt_ref as string }), skill_refs: row.skill_refs as string[], mcp_refs: row.mcp_refs as string[], resource_refs: row.resource_refs as string[] };
  });
  if (!Array.isArray(root.resources) || root.resources.length > 32) fail("input", "deployment_invalid");
  const resourceIds = new Set<string>();
  const resources = root.resources.map((raw): Ref => {
    const row = obj(raw); exact(row, ["id", "origin", "token_env", "expected_version", "node_id"]);
    if (!identifier(row.id) || resourceIds.has(row.id) || !identifier(row.node_id) || !tokenEnv(row.token_env) || !Number.isSafeInteger(row.expected_version) || (row.expected_version as number) < 0) fail("input", "resource_ref_invalid");
    resourceIds.add(row.id);
    return { id: row.id, origin: providerOrigin(row.origin), token_env: row.token_env, expected_version: row.expected_version as number, node_id: row.node_id };
  });
  for (const agent of agents) if (agent.resource_refs.some((id) => !resourceIds.has(id))) fail("input", "resource_ref_invalid");
  const bundle = obj(root.bundle); exact(bundle, ["package_dir", "bindings_file"]);
  relativePath(bundle.package_dir); relativePath(bundle.bindings_file);
  return { schema: DEPLOYMENT_SCHEMA, id: root.id, runtime: { origin: providerOrigin(runtime.origin), token_env: runtime.token_env }, core: { origin: coreOrigin(String(core.origin)) }, profiles, agents, resources, bundle: { package_dir: bundle.package_dir as string, bindings_file: bundle.bindings_file as string } };
}
function buildPayload(agent: AgentDecl, profile: JsonRecord, instructions: string): JsonRecord {
  const merged = { ...profile, ...agent.config };
  const required = ["name", "kind"];
  if (required.some((key) => !(key in merged)) || typeof merged.name !== "string" || !merged.name.trim() || typeof merged.kind !== "string") fail("input", "agent_config_invalid");
  const payload: JsonRecord = {
    tool_approval: merged.tool_approval, name: merged.name, description: merged.description ?? "", kind: merged.kind,
    engine: merged.engine, model_parameters: merged.model_parameters, provider: merged.provider ?? "openai-compatible",
    version: agent.expected_version, id: agent.id, instructions, model: merged.model ?? "", credential_env: merged.credential_env ?? "OPENAI_API_KEY",
    base_url: merged.base_url ?? DEFAULT_BASE_URL, statuses: merged.statuses ?? ["success", "needs_help"], events: merged.events ?? [],
    mcp_ids: agent.mcp_refs, skills: agent.skill_refs, rules: merged.rules ?? [],
  };
  for (const key of Object.keys(payload)) if (payload[key] === undefined) delete payload[key];
  if (Buffer.byteLength(JSON.stringify(payload), "utf8") > 100_000) fail("input", "agent_config_invalid");
  return validateManagedAgentPayload(payload);
}
// Mirrors @roster/contracts 0.49.0 ManagedAgent, AgentEngine, ModelProvider,
// ModelParameters, and ToolApprovalPolicy. Keep the regression cases in
// dashboard.test.ts aligned when that pinned contracts schema changes.
function validModelParameterValue(value: unknown, depth: number): boolean {
  if (depth > 8) return false;
  if (value === null || typeof value === "boolean" || typeof value === "string") return true;
  if (typeof value === "number") return Number.isFinite(value);
  if (Array.isArray(value)) return value.length <= 100 && value.every((item) => validModelParameterValue(item, depth + 1));
  if (!value || typeof value !== "object") return false;
  return Object.entries(value).every(([key, item]) => key.length <= 100 && !MODEL_PARAMETER_RESERVED.test(key) && validModelParameterValue(item, depth + 1));
}
function validModelParameters(value: unknown): value is JsonRecord {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  try { return Object.keys(value).length <= 64 && JSON.stringify(value).length <= 16_000 && validModelParameterValue(value, 0); }
  catch { return false; }
}
function stringArray(value: unknown, validate: (item: string) => boolean = () => true): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === "string" && validate(item));
}
function validateManagedAgentPayload(value: JsonRecord): JsonRecord {
  if (Object.keys(value).some((key) => !AGENT_FIELDS_COMPLETE.has(key)) || [...AGENT_FIELDS_COMPLETE].filter((key) => key !== "engine" && key !== "model_parameters" && key !== "tool_approval").some((key) => !Object.hasOwn(value, key))) fail("input", "agent_config_invalid");
  if (!identifier(value.id) || typeof value.name !== "string" || value.name.length < 1 || typeof value.description !== "string" ||
    (value.kind !== "echo" && value.kind !== "llm") || !Number.isSafeInteger(value.version) || (value.version as number) < 0 ||
    typeof value.instructions !== "string" || value.instructions.length > 20_000 || typeof value.model !== "string" || value.model.trim().length > 300 ||
    typeof value.credential_env !== "string" || !/^(?:[A-Z][A-Z0-9_]*|)$/u.test(value.credential_env) || typeof value.base_url !== "string") fail("input", "agent_config_invalid");
  try { new URL(value.base_url); } catch { fail("input", "agent_config_invalid"); }
  if (value.engine !== undefined && (typeof value.engine !== "string" || !AGENT_ENGINE.has(value.engine)) ||
    value.model_parameters !== undefined && !validModelParameters(value.model_parameters) ||
    value.tool_approval !== undefined && !validToolApproval(value.tool_approval) ||
    !stringArray(value.statuses, identifier) || value.statuses.length < 1 || !stringArray(value.events, identifier) ||
    !stringArray(value.mcp_ids, identifier) || !stringArray(value.skills) || !stringArray(value.rules)) fail("input", "agent_config_invalid");
  const provider = typeof value.provider === "string" ? value.provider.trim() : "";
  value.provider = provider;
  value.model = value.model.trim();
  if (!/^[a-z][a-z0-9_-]{0,99}$/u.test(provider)) fail("input", "agent_config_invalid");
  return value;
}
function validToolApproval(value: unknown): boolean {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const policy = value as JsonRecord;
  return Object.keys(policy).every((key) => key === "mode" || key === "tools") &&
    (policy.mode === "none" || policy.mode === "all" || policy.mode === "selected") &&
    Array.isArray(policy.tools) && policy.tools.length <= 200 && policy.tools.every((tool) => typeof tool === "string" && tool.length >= 1 && tool.length <= 200) &&
    (policy.mode !== "selected" || policy.tools.length > 0);
}
function headers(token: string): HeadersInit { return { accept: "application/json", authorization: `Bearer ${token}` }; }
async function request(origin: string, route: string, token: string, deadline: number, method = "GET", body?: unknown, send?: () => void): Promise<{ status: number; value: unknown }> {
  const remain = deadline - Date.now(); if (remain <= 0) fail("preflight", "overall_timeout", 4);
  const controller = new AbortController(); const timer = setTimeout(() => controller.abort(), Math.min(15_000, remain));
  try {
    send?.();
    const response = await fetch(origin + route, { method, redirect: "manual", signal: controller.signal,
      headers: body === undefined ? headers(token) : { ...headers(token), "content-type": "application/json" }, ...(body === undefined ? {} : { body: JSON.stringify(body) }) });
    if (response.status >= 300 && response.status < 400) { if (response.body) void response.body.cancel().catch(() => {}); return { status: response.status, value: null }; }
    if (!response.body) return { status: response.status, value: null };
    const reader = response.body.getReader(); const chunks: Uint8Array[] = []; let size = 0;
    try { while (true) { const part = await reader.read(); if (part.done) break; size += part.value.byteLength; if (size > 2 * 1024 * 1024) throw new Error("limit"); chunks.push(part.value); } }
    catch (error) { void reader.cancel().catch(() => {}); throw error; }
    finally { reader.releaseLock(); }
    let value: unknown = null;
    if (chunks.length) { try { value = parseStrictJson(new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(chunks))); } catch { value = null; } }
    return { status: response.status, value };
  } catch { fail("preflight", controller.signal.aborted ? "request_timeout" : "service_unavailable", 4); }
  finally { clearTimeout(timer); }
}
function recordOrFail(value: unknown): JsonRecord { if (!value || typeof value !== "object" || Array.isArray(value)) fail("preflight", "service_response_invalid", 4); return value as JsonRecord; }
function requireOk(response: { status: number; value: unknown }, service: string): JsonRecord {
  if (response.status >= 400 && response.status < 500) fail("preflight", `${service}_rejected`);
  if (response.status !== 200) fail("preflight", `${service}_unavailable`, 4);
  return recordOrFail(response.value);
}
function effectiveAgent(row: unknown): JsonRecord { return recordOrFail(row); }
function deepEqual(a: unknown, b: unknown): boolean { return canonical(a) === canonical(b); }
function ensureAgentMatches(current: JsonRecord, desired: JsonRecord, expectedVersion: number): boolean {
  if (current.version !== expectedVersion + 1 || current.id !== desired.id) return false;
  for (const [key, value] of Object.entries(desired)) if (key !== "version" && !deepEqual(current[key], value)) return false;
  return true;
}
async function preflight(file: string, deadline: number, onDeclared?: (id: string) => void): Promise<Preflight> {
  const absoluteFile = path.resolve(file); const { text } = await readUtf8File(absoluteFile, 256 * 1024);
  let raw: unknown; try { raw = parseStrictJson(text); } catch { fail("input", "json_invalid"); }
  depthCheck(raw); const declaration = normalizeDeployment(raw);
  onDeclared?.(declaration.id);
  const base = path.dirname(absoluteFile);
  const packagePath = resolveRelative(base, declaration.bundle.package_dir);
  const bindingPath = resolveRelative(base, declaration.bundle.bindings_file);
  const runtimeToken = process.env[declaration.runtime.token_env];
  if (!runtimeToken) fail("input", "token_missing");
  const resourceTokens = new Map<string, string>();
  for (const resource of declaration.resources) { const token = process.env[resource.token_env]; if (!token) fail("input", "token_missing"); resourceTokens.set(resource.id, token); }
  const agentPlans: AgentPlan[] = []; const promptHashes: Record<string, string | null> = Object.create(null) as Record<string, string | null>;
  for (const agent of declaration.agents) {
    let prompt = ""; let promptHash: string | null = null;
    if (agent.prompt_ref) {
      const resolved = resolveRelative(base, agent.prompt_ref);
      const promptFile = await readUtf8File(resolved, 80 * 1024); prompt = promptFile.text; if (prompt.length > 20_000) fail("input", "prompt_size_limit"); promptHash = sha(promptFile.bytes);
    }
    promptHashes[agent.id] = promptHash;
    agentPlans.push({ id: agent.id, version: agent.expected_version, payload: buildPayload(agent, declaration.profiles[agent.profile_ref]!, prompt), prompt_hash: promptHash });
  }
  const runtimeOrigin = declaration.runtime.origin;
  const [adminResponse, mcpResponse] = await Promise.all([
    request(runtimeOrigin, "/admin", runtimeToken, deadline), request(runtimeOrigin, "/mcp-catalogue", runtimeToken, deadline),
  ]);
  const admin = requireOk(adminResponse, "runtime"); const mcpCatalogue = requireOk(mcpResponse, "mcp_catalogue");
  const manifest = recordOrFail(admin.manifest); const runtimeId = manifest.runtime_id;
  if (!identifier(runtimeId) || manifest.protocol !== "roster/0.1" || mcpCatalogue.protocol !== "roster-mcp-catalogue/1" || mcpCatalogue.runtime_id !== runtimeId) fail("preflight", "runtime_identity_invalid", 4);
  if (!Array.isArray(admin.managed_agents) || !Array.isArray(manifest.agents) || !Array.isArray(mcpCatalogue.connections)) fail("preflight", "runtime_response_invalid", 4);
  const managed = new Map<string, JsonRecord>();
  for (const row of admin.managed_agents) { const value = recordOrFail(row); if (!identifier(value.id) || managed.has(value.id)) fail("preflight", "runtime_response_invalid", 4); managed.set(value.id, value); }
  const manifestAgentIds = new Set<string>();
  for (const rawRow of manifest.agents) { const row = recordOrFail(rawRow); if (!identifier(row.id) || manifestAgentIds.has(row.id)) fail("preflight", "runtime_response_invalid", 4); manifestAgentIds.add(row.id); }
  const skills = recordOrFail(admin.skill_catalog); const skillRows = Array.isArray(skills.skills) ? skills.skills.map(recordOrFail) : [];
  for (const agent of declaration.agents) {
    const current = managed.get(agent.id); const actualVersion = current?.version ?? 0;
    if (actualVersion !== agent.expected_version) fail("preflight", "agent_version_conflict");
    if (current && !manifestAgentIds.has(agent.id) || !current && manifestAgentIds.has(agent.id)) fail("preflight", "agent_version_conflict");
    if (agent.skill_refs.length) {
      if (skills.mode !== "files" || skills.error || skills.truncated === true || agent.skill_refs.some((id) => !skillRows.some((skill) => skill.id === id && skill.ready === true))) fail("preflight", "skill_reference_invalid");
    }
    if (agent.mcp_refs.some((id) => !(mcpCatalogue.connections as unknown[]).some((connection) => { const row = recordOrFail(connection); return row.connection_id === id && row.state === "active"; }))) fail("preflight", "mcp_reference_invalid");
  }
  const resources = declaration.resources;
  const resourceSnapshots = await Promise.all(resources.map(async (resource) => {
    const response = await request(resource.origin, `/v1/resources/${encodeURIComponent(resource.id)}`, resourceTokens.get(resource.id)!, deadline);
    const body = requireOk(response, "resource");
    if (body.id !== resource.id || body.version !== resource.expected_version) fail("preflight", "resource_version_conflict");
    return { id: resource.id, version: body.version };
  }));
  const bundle = await prepareBundle(packagePath, bindingPath, declaration.core.origin, deadline);
  const resourceNodes = new Map(bundle.manifest.nodes.map((node) => [node.id, node]));
  for (const resource of resources) {
    const node = resourceNodes.get(resource.node_id); const binding = node && bundle.bindings[node.binding_ref];
    if (!node || node.connector_id !== "resource" || !binding || binding.target_id !== resource.id || binding.url !== resource.origin || binding.token_env !== resource.token_env) fail("preflight", "resource_node_mismatch");
  }
  for (const agent of declaration.agents) for (const id of agent.resource_refs) if (!resources.some((resource) => resource.id === id && bundle.manifest.nodes.some((node) => node.id === resource.node_id))) fail("preflight", "resource_node_mismatch");
  const referencedSkills = [...new Set(declaration.agents.flatMap((agent) => agent.skill_refs))].sort();
  const referencedMcp = [...new Set(declaration.agents.flatMap((agent) => agent.mcp_refs))].sort();
  const digestInput = { declaration: JSON.parse(canonical(declaration)), prompt_hashes: promptHashes,
    runtime: { id: runtimeId, agents: declaration.agents.map((agent) => ({ id: agent.id, version: managed.get(agent.id)?.version ?? 0 })),
      skills: { mode: skills.mode, refs: referencedSkills.map((id) => ({ id, ready: skillRows.some((skill) => skill.id === id && skill.ready === true) })) },
      mcp: referencedMcp.map((id) => { const connection = (mcpCatalogue.connections as unknown[]).map(recordOrFail).find((row) => row.connection_id === id)!; return { connection_id: id, revision: connection.revision, state: connection.state }; }) },
    resources: resourceSnapshots, bundle_plan: bundle.digest };
  return { declaration, runtimeOrigin, runtimeId, coreOrigin: declaration.core.origin, runtimeToken, resourceTokens, agents: agentPlans, resources, bundle, digest: sha(canonical(digestInput)) };
}
function output(deploymentId: string, status: Result["status"], digest: string | null, applied: Entry[], unresolved: Unresolved[], code: string): Result {
  return { schema: RESULT_SCHEMA, deployment_id: deploymentId, status, digest, applied, unresolved, code };
}
function assertBundleNodeLink(plan: BundlePlan, declaration: Deployment): void {
  const nodes = new Map(plan.manifest.nodes.map((node) => [node.id, node]));
  for (const resource of declaration.resources) { const node = nodes.get(resource.node_id); const binding = node && plan.bindings[node.binding_ref]; if (!node || node.connector_id !== "resource" || !binding || binding.target_id !== resource.id || binding.url !== resource.origin || binding.token_env !== resource.token_env) fail("preflight", "resource_node_mismatch"); }
}
export async function runDeployment(argv: string[]): Promise<{ code: number; result: Result }> {
  const started = Date.now(); let deploymentId = ""; let digest: string | null = null; const applied: Entry[] = []; const unresolved: Unresolved[] = [];
  try {
    if (argv.length < 2 || (argv[0] !== "validate" && argv[0] !== "apply")) fail("arguments", "usage_invalid");
    const action = argv[0]!; const values = new Map<string, string>();
    for (let i = 1; i < argv.length; i++) { const key = argv[i]!; if (!(["--file", "--expected-digest"] as string[]).includes(key) || values.has(key) || !argv[i + 1] || argv[i + 1]!.startsWith("--")) fail("arguments", "usage_invalid"); values.set(key, argv[++i]!); }
    if (!values.has("--file") || action === "validate" && values.has("--expected-digest") || action === "apply" && !/^[a-f0-9]{64}$/u.test(values.get("--expected-digest") ?? "")) fail("arguments", "usage_invalid");
    const deadline = started + 120_000;
    let plan: Preflight;
    try { plan = await preflight(values.get("--file")!, deadline, (id) => { deploymentId = id; }); }
    catch (error) { if (error instanceof BundleError && error.exitCode === 4) throw new BundleError(error.stage, error.code, error.id, 2); throw error; }
    digest = plan.digest;
    assertBundleNodeLink(plan.bundle, plan.declaration);
    if (action === "validate") return { code: 0, result: output(deploymentId, "planned", digest, [], [], "planned") };
    if (values.get("--expected-digest") !== plan.digest) fail("digest", "digest_mismatch");
    for (const agent of plan.agents) {
      let sent = false;
      try {
        const response = await request(plan.runtimeOrigin, "/admin/agents", plan.runtimeToken, deadline, "PUT", agent.payload, () => { sent = true; });
        if (response.status >= 400 && response.status < 500) {
          const status = applied.length ? "partial" : "failed";
          return { code: 3, result: output(deploymentId, status, digest, applied, unresolved, "runtime_rejected") };
        }
        if (response.status !== 200) throw new Error("unknown");
        const putManifest = recordOrFail(response.value);
        if (putManifest.protocol !== "roster/0.1" || putManifest.runtime_id !== plan.runtimeId || !Array.isArray(putManifest.agents) ||
          !putManifest.agents.some((raw) => recordOrFail(raw).id === agent.id)) throw new Error("unknown");
        const check = await request(plan.runtimeOrigin, "/admin", plan.runtimeToken, deadline);
        const admin = requireOk(check, "runtime");
        const confirmedManifest = recordOrFail(admin.manifest);
        if (!Array.isArray(admin.managed_agents) || confirmedManifest.protocol !== "roster/0.1" || confirmedManifest.runtime_id !== plan.runtimeId) throw new Error("unknown");
        const current = admin.managed_agents.map(recordOrFail).find((row) => row.id === agent.id);
        if (!current || !ensureAgentMatches(current, agent.payload, agent.version)) throw new Error("unknown");
        applied.push({ service: "runtime", id: agent.id, revision: agent.version + 1 });
      } catch {
        if (sent) unresolved.push({ service: "runtime", id: agent.id });
        return { code: 4, result: output(deploymentId, sent || applied.length ? "partial" : "failed", digest, applied, unresolved, sent ? "runtime_unconfirmed" : "runtime_unavailable") };
      }
    }
    const bundleResult = await applyBundle(plan.bundle, plan.coreOrigin, deadline);
    applied.push(...bundleResult.applied.map((action) => ({ service: "core", id: action.id, revision: null })));
    unresolved.push(...bundleResult.unresolved.map((action) => ({ service: "core", id: action.id })));
    const bundleCode = applyExitCode(bundleResult);
    if (bundleCode === 0) return { code: 0, result: output(deploymentId, "applied", digest, applied, unresolved, "applied") };
    return { code: bundleResult.unresolved.length ? 4 : 3, result: output(deploymentId, "partial", digest, applied, unresolved, bundleResult.failure?.code ?? "core_partial") };
  } catch (error) {
    const failure = error instanceof BundleError ? error : new BundleError("input", "deployment_invalid");
    return { code: failure.exitCode, result: output(deploymentId, failure.exitCode === 4 && applied.length ? "partial" : "failed", digest, applied, unresolved, failure.code) };
  }
}
