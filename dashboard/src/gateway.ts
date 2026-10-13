import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { readFile, stat } from "node:fs/promises";
import { extname, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import {
  parseCallsQuery,
  parseListLimit,
  parseLogsQuery,
  projectCall,
  projectCalls,
  projectConversations,
  projectCore,
  projectLogs,
  projectNodeDetails,
  projectNodes,
  projectRelations,
  validDashboardRequest,
  type ApiMeta,
  type CallsResponse,
  type OverviewResponse,
  type SafeCall,
} from "./projection.js";

const CORE = "http://127.0.0.1:7350";
const PORT = Number(process.env.PORT ?? "7360");
const ROOT = resolve(fileURLToPath(new URL("../../dist", import.meta.url)));
const MAX_BODY = 2 * 1024 * 1024;
const REQUEST_TIMEOUT = 5_000;

function meta(errors: Record<string, string>, count: number, truncated: boolean): ApiMeta {
  return { fetched_at: new Date().toISOString(), stale: Object.keys(errors).length > 0,
    returned_count: count, truncated, errors };
}

function sendJson(response: ServerResponse, status: number, value: unknown): void {
  response.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
    "X-Content-Type-Options": "nosniff",
    "Content-Security-Policy": "default-src 'none'; frame-ancestors 'none'",
  });
  response.end(JSON.stringify(value));
}

function callCore(command: string, args: unknown): Promise<unknown> {
  return fetch(`${CORE}/api/commands/${command}`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Origin: CORE, "Sec-Fetch-Site": "same-origin" },
    body: JSON.stringify(args),
    redirect: "manual",
    signal: AbortSignal.timeout(REQUEST_TIMEOUT),
  }).then(async (response) => {
    if (!response.ok) throw new Error(`core_http_${response.status}`);
    if (!response.body) throw new Error("core_invalid_json");
    const reader = response.body.getReader();
    const chunks: Uint8Array[] = [];
    let size = 0;
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > MAX_BODY) { await reader.cancel(); throw new Error("core_response_too_large"); }
      chunks.push(value);
    }
    const joined = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) { joined.set(chunk, offset); offset += chunk.byteLength; }
    const reviver = ((key: string, value: unknown, context?: { source?: string }) => {
      if ((key === "sequence" || key === "before_revision" || key === "after_revision") &&
          typeof value === "number" && !Number.isSafeInteger(value) && context?.source &&
          /^(0|[1-9][0-9]*)$/u.test(context.source)) {
        return { dashboard_unsafe_integer: true, source: context.source };
      }
      return value;
    }) as Parameters<typeof JSON.parse>[1];
    return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(joined), reviver) as unknown;
  });
}

function errorCode(error: unknown): string {
  if (error instanceof Error && error.message.startsWith("core_http_")) return error.message;
  if (error instanceof Error && error.message === "core_response_too_large") return error.message;
  if (error instanceof SyntaxError) return "core_invalid_json";
  return "core_unavailable";
}

async function overview(): Promise<OverviewResponse> {
  const errors: Record<string, string> = {};
  const commands = ["core.status", "nodes.list", "calls.list"] as const;
  const outcomes = await Promise.all(commands.map(async (command) => {
    const attempted_at = new Date().toISOString();
    try { return { ok: true as const, attempted_at, value: await callCore(command, {}) }; }
    catch (error) { return { ok: false as const, attempted_at, error }; }
  }));
  let core = null;
  let nodes = null;
  let calls: SafeCall[] | null = null;
  let nodesTruncated = false;
  let callsTruncated = false;
  const sources: OverviewResponse["sources"] = {
    core: { attempted_at: "", error: null, returned_count: 0, truncated: false },
    nodes: { attempted_at: "", error: null, returned_count: 0, truncated: false },
    calls: { attempted_at: "", error: null, returned_count: 0, truncated: false },
  };
  const keys = ["core", "nodes", "calls"] as const;
  for (let index = 0; index < outcomes.length; index += 1) {
    const outcome = outcomes[index]!;
    const key = keys[index]!;
    sources[key].attempted_at = outcome.attempted_at;
    if (!outcome.ok) { const code = errorCode(outcome.error); errors[key] = code; sources[key].error = code; continue; }
    try {
      if (key === "core") { core = projectCore(outcome.value); sources.core.returned_count = 1; }
      else if (key === "nodes") {
        if (!Array.isArray(outcome.value)) throw new Error("invalid_nodes");
        nodesTruncated = outcome.value.length > 100;
        nodes = projectNodes(outcome.value, 100);
        sources.nodes.returned_count = nodes.length;
        sources.nodes.truncated = nodesTruncated;
      }
      else {
        if (!Array.isArray(outcome.value)) throw new Error("invalid_calls");
        callsTruncated = outcome.value.length > 12;
        calls = projectCalls(outcome.value, 12);
        sources.calls.returned_count = calls.length;
        sources.calls.truncated = callsTruncated;
      }
    } catch { errors[key] = "core_projection_invalid"; sources[key].error = errors[key]; }
  }
  return { meta: meta(errors, (nodes?.length ?? 0) + (calls?.length ?? 0), nodesTruncated || callsTruncated), sources, core, nodes, calls };
}

async function calls(query: ReturnType<typeof parseCallsQuery>): Promise<CallsResponse> {
  const args: Record<string, string> = {};
  if (query.node_id) args.node_id = query.node_id;
  if (query.correlation_id) args.correlation_id = query.correlation_id;
  if (query.state) args.state = query.state;
  try {
    const raw = await callCore("calls.list", args);
    const all = projectCalls(raw, Number.MAX_SAFE_INTEGER);
    const filtered = all.filter((call) =>
      (!query.node_id || call.call.target.id === query.node_id) &&
      (!query.correlation_id || call.call.correlation_id === query.correlation_id) &&
      (!query.state || call.lifecycle.state === query.state));
    const selected = filtered.slice(0, query.limit);
    return { meta: meta({}, selected.length, filtered.length > selected.length), calls: selected };
  } catch (error) {
    const errors = { calls: errorCode(error) };
    return { meta: meta(errors, 0, false), calls: null };
  }
}

async function nodes(): Promise<{ meta: ApiMeta; nodes: ReturnType<typeof projectNodeDetails> | null }> {
  try {
    const raw = await callCore("nodes.list", {});
    if (!Array.isArray(raw)) throw new Error("core_projection_invalid");
    const projected = projectNodeDetails(raw, 100);
    return { meta: meta({}, projected.length, raw.length > projected.length), nodes: projected };
  } catch (error) {
    const errors = { nodes: errorCode(error) === "core_unavailable" ? "core_projection_invalid" : errorCode(error) };
    return { meta: meta(errors, 0, false), nodes: null };
  }
}

async function relations(): Promise<{ meta: ApiMeta; relations: ReturnType<typeof projectRelations> | null }> {
  try {
    const raw = await callCore("subscriptions.list", {});
    if (!Array.isArray(raw)) throw new Error("core_projection_invalid");
    const projected = projectRelations(raw, 100);
    return { meta: meta({}, projected.length, raw.length > projected.length), relations: projected };
  } catch (error) {
    const errors = { relations: errorCode(error) === "core_unavailable" ? "core_projection_invalid" : errorCode(error) };
    return { meta: meta(errors, 0, false), relations: null };
  }
}

async function conversations(limit: number): Promise<{ meta: ApiMeta; conversations: ReturnType<typeof projectConversations> | null }> {
  try {
    const raw = await callCore("conversations.list", {});
    if (!Array.isArray(raw)) throw new Error("core_projection_invalid");
    const projected = projectConversations(raw, limit);
    return { meta: meta({}, projected.length, raw.length > projected.length), conversations: projected };
  } catch (error) {
    const errors = { conversations: errorCode(error) === "core_unavailable" ? "core_projection_invalid" : errorCode(error) };
    return { meta: meta(errors, 0, false), conversations: null };
  }
}

async function logs(query: ReturnType<typeof parseLogsQuery>): Promise<{
  meta: ApiMeta; entries: ReturnType<typeof projectLogs>["entries"] | null; next_cursor: string; cursor_expired: boolean;
}> {
  const args: Record<string, string | number> = { limit: query.limit };
  if (query.call_id) args.call_id = query.call_id;
  if (query.correlation_id) args.correlation_id = query.correlation_id;
  if (query.conversation_id) args.conversation_id = query.conversation_id;
  if (query.after !== undefined) args.after = query.after;
  try {
    const page = projectLogs(await callCore("logs.query", args), query.limit);
    return { meta: meta({}, page.entries.length, page.entries.length >= query.limit), entries: page.entries,
      next_cursor: page.next_cursor, cursor_expired: false };
  } catch (error) {
    const code = errorCode(error);
    const expired = code === "core_http_410";
    const errors = { logs: expired ? "cursor_expired" : code === "core_unavailable" ? "core_projection_invalid" : code };
    return { meta: meta(errors, 0, false), entries: null, next_cursor: "", cursor_expired: expired };
  }
}

async function serveStatic(pathname: string, response: ServerResponse): Promise<void> {
  let relative: string;
  try { relative = pathname === "/" ? "index.html" : decodeURIComponent(pathname.slice(1)); }
  catch { sendJson(response, 404, { error: "not_found" }); return; }
  const candidate = resolve(ROOT, relative);
  if (candidate !== ROOT && !candidate.startsWith(`${ROOT}${sep}`)) {
    sendJson(response, 404, { error: "not_found" }); return;
  }
  try {
    const info = await stat(candidate);
    if (!info.isFile()) throw new Error("not_file");
    const content = await readFile(candidate);
    const types: Record<string, string> = { ".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8", ".css": "text/css; charset=utf-8", ".svg": "image/svg+xml" };
    response.writeHead(200, {
      "Content-Type": types[extname(candidate)] ?? "application/octet-stream",
      "Cache-Control": "no-cache", "X-Content-Type-Options": "nosniff",
      "Referrer-Policy": "no-referrer", "X-Frame-Options": "DENY",
      "Content-Security-Policy": "default-src 'self'; connect-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; script-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'",
    });
    response.end(content);
  } catch { sendJson(response, 404, { error: "not_found" }); }
}

const server = createServer(async (request: IncomingMessage, response: ServerResponse) => {
  let url: URL;
  try { url = new URL(request.url ?? "/", `http://${request.headers.host ?? "invalid"}`); }
  catch { sendJson(response, 400, { error: "invalid_request_target" }); return; }
  if (!validDashboardRequest({
    remoteAddress: request.socket.remoteAddress, host: request.headers.host,
    origin: request.headers.origin, fetchSite: request.headers["sec-fetch-site"], port: PORT,
  })) { sendJson(response, 403, { error: "local_origin_required" }); return; }
  if (request.method === "GET" && url.pathname === "/api/overview") {
    if (url.search !== "") { sendJson(response, 400, { error: "invalid_query" }); return; }
    sendJson(response, 200, await overview()); return;
  }
  if (request.method === "GET" && url.pathname === "/api/calls") {
    try { sendJson(response, 200, await calls(parseCallsQuery(url.searchParams))); }
    catch { sendJson(response, 400, { error: "invalid_query" }); }
    return;
  }
  if (request.method === "GET" && url.pathname === "/api/nodes") {
    if (url.search !== "") { sendJson(response, 400, { error: "invalid_query" }); return; }
    sendJson(response, 200, await nodes()); return;
  }
  if (request.method === "GET" && url.pathname === "/api/relations") {
    if (url.search !== "") { sendJson(response, 400, { error: "invalid_query" }); return; }
    sendJson(response, 200, await relations()); return;
  }
  if (request.method === "GET" && url.pathname === "/api/conversations") {
    try { sendJson(response, 200, await conversations(parseListLimit(url.searchParams, ["limit"], 50, 100))); }
    catch { sendJson(response, 400, { error: "invalid_query" }); }
    return;
  }
  if (request.method === "GET" && url.pathname === "/api/logs") {
    try { sendJson(response, 200, await logs(parseLogsQuery(url.searchParams))); }
    catch { sendJson(response, 400, { error: "invalid_query" }); }
    return;
  }
  if (request.method !== "GET" || url.pathname.startsWith("/api/")) {
    sendJson(response, 404, { error: "not_found" }); return;
  }
  await serveStatic(url.pathname, response);
});

server.listen(PORT, "127.0.0.1");
