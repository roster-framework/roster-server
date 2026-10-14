import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { readFile, stat } from "node:fs/promises";
import { extname, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { parseStrictJson } from "./bundle.js";
import {
  MutationInputError,
  parseCallAbortInput,
  parseSubscriptionCreateInput,
  parseSubscriptionRemoveInput,
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
const MAX_MUTATION_BODY = 32 * 1024;
const MAX_ABORT_BODY = 4 * 1024;
const REQUEST_TIMEOUT = 5_000;

class MutationRequestError extends Error { constructor(readonly status: number, readonly code: string) { super(code); } }

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

function readMutationBody(request: IncomingMessage, maxBytes = MAX_MUTATION_BODY): Promise<unknown> {
  return new Promise((resolveBody, rejectBody) => {
    const chunks: Buffer[] = []; let size = 0; let settled = false;
    const cleanup = () => { clearTimeout(timer); request.off("data", onData); request.off("end", onEnd); request.off("aborted", onAborted); request.off("error", onError); };
    const fail = (status: number, code: string) => { if (settled) return; settled = true; cleanup(); rejectBody(new MutationRequestError(status, code)); };
    const onData = (chunk: Buffer | string) => {
      const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      size += bytes.byteLength;
      if (size > maxBytes) { request.pause(); fail(413, "body_too_large"); return; }
      chunks.push(bytes);
    };
    const onEnd = () => {
      if (settled) return;
      settled = true; cleanup();
      try { resolveBody(parseStrictJson(new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(chunks)))); }
      catch { rejectBody(new MutationRequestError(400, "invalid_json")); }
    };
    const onAborted = () => fail(400, "body_incomplete");
    const onError = () => fail(400, "body_incomplete");
    const timer = setTimeout(() => { request.pause(); fail(408, "body_timeout"); }, REQUEST_TIMEOUT);
    request.on("data", onData); request.once("end", onEnd); request.once("aborted", onAborted); request.once("error", onError);
  });
}

function closeAfterMutationError(request: IncomingMessage, response: ServerResponse, status: number, code: string): void {
  sendJson(response, status, { error: code });
  if (status === 408 || status === 413) response.once("finish", () => request.destroy());
}

function validateMutationContentType(value: string | undefined): boolean {
  return value !== undefined && /^application\/json(?:\s*;\s*charset\s*=\s*(?:utf-8|"utf-8"))?\s*$/iu.test(value);
}

async function readMutationResponse(response: Response, signal: AbortSignal): Promise<unknown> {
  if (!response.body) throw new Error("core_response_invalid");
  const reader = response.body.getReader(); const chunks: Uint8Array[] = []; let size = 0;
  try {
    while (true) {
      const part = await reader.read(); if (part.done) break;
      size += part.value.byteLength;
      if (size > MAX_BODY) throw new Error("core_response_too_large");
      chunks.push(part.value);
    }
  } catch (error) {
    void reader.cancel().catch(() => {});
    throw error;
  } finally { reader.releaseLock(); }
  if (signal.aborted) throw new DOMException("timeout", "TimeoutError");
  try { return parseStrictJson(new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(chunks))); }
  catch { throw new Error("core_response_invalid"); }
}

async function sendMutationCommand(command: "subscriptions.create" | "subscriptions.remove" | "calls.abort", args: unknown): Promise<{ status: number; value: unknown }> {
  const controller = new AbortController(); const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
  let sendStarted = false;
  try {
    sendStarted = true;
    const response = await fetch(`${CORE}/api/commands/${command}`, {
      method: "POST", headers: { "Content-Type": "application/json", Origin: CORE, "Sec-Fetch-Site": "same-origin" },
      body: JSON.stringify(args), redirect: "manual", signal: controller.signal,
    });
    const value = await readMutationResponse(response, controller.signal);
    if (response.status >= 300 && response.status < 400) throw new Error("core_response_invalid");
    return { status: response.status, value };
  } catch (error) {
    if (sendStarted && (controller.signal.aborted || (error instanceof Error && error.name === "TimeoutError"))) throw new MutationRequestError(504, "mutation_timeout");
    throw new MutationRequestError(502, "mutation_unconfirmed");
  } finally { clearTimeout(timer); }
}

const faultCodes: Record<number, Set<string>> = {
  400: new Set(["invalid", "validation", "invalid_input", "invalid_schema"]),
  404: new Set(["not_found"]),
  403: new Set(["forbidden"]),
  409: new Set(["conflict"]),
  422: new Set(["unsupported"]),
  429: new Set(["busy"]),
};

function coreFaultCode(value: unknown, status: number): string | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const code = (value as Record<string, unknown>).code;
  return typeof code === "string" && /^[a-z][a-z0-9_]{0,63}$/u.test(code) && faultCodes[status]?.has(code) ? code : null;
}

function subscriptionSnapshot(value: unknown, expected: ReturnType<typeof parseSubscriptionCreateInput>): ReturnType<typeof projectRelations>[number] {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("response_invalid");
  const snapshot = value as Record<string, unknown>;
  const subject = snapshot.subject as Record<string, unknown> | null;
  if (!subject || subject.namespace !== "subscriptions" || subject.id !== expected.id || snapshot.revision !== 1) throw new Error("response_invalid");
  const rawValue = snapshot.value;
  if (!rawValue || typeof rawValue !== "object" || Array.isArray(rawValue)) throw new Error("response_invalid");
  const rawRemoved = (rawValue as Record<string, unknown>).removed;
  if (rawRemoved !== undefined && rawRemoved !== false) throw new Error("response_invalid");
  const relation = projectRelations([value], 1)[0];
  if (!relation || relation.id !== expected.id || relation.removed || relation.source.id !== expected.source.id || relation.source.namespace !== "nodes" ||
      relation.event !== expected.event || relation.target.id !== expected.target.id || relation.target.namespace !== "nodes" || relation.operation !== expected.operation) throw new Error("response_invalid");
  return relation;
}

function removedSubscription(value: unknown, expected: ReturnType<typeof parseSubscriptionRemoveInput>): { id: string; revision: number; removed: true } {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("response_invalid");
  const change = value as Record<string, unknown>; const subject = change.subject as Record<string, unknown> | null;
  if (!subject || subject.namespace !== "subscriptions" || subject.id !== expected.id ||
      change.before_revision !== expected.expected_revision || !Number.isSafeInteger(change.after_revision) || change.after_revision !== expected.expected_revision + 1) throw new Error("response_invalid");
  const relation = projectRelations([{ subject, revision: change.after_revision, value: change.after }], 1)[0];
  if (!relation || relation.id !== expected.id || !relation.removed) throw new Error("response_invalid");
  return { id: expected.id, revision: relation.revision, removed: true };
}

async function mutateSubscription(request: IncomingMessage, response: ServerResponse, action: "create" | "remove"): Promise<void> {
  if (request.url?.includes("?")) { sendJson(response, 400, { error: "invalid_query" }); return; }
  if (request.headers.origin === undefined) { sendJson(response, 403, { error: "local_origin_required" }); return; }
  if (!validateMutationContentType(request.headers["content-type"])) { sendJson(response, 415, { error: "content_type_required" }); return; }
  const length = request.headers["content-length"];
  if (length !== undefined && (!/^[0-9]+$/u.test(length) || Number(length) > MAX_MUTATION_BODY)) { closeAfterMutationError(request, response, 413, "body_too_large"); return; }
  let input: ReturnType<typeof parseSubscriptionCreateInput> | ReturnType<typeof parseSubscriptionRemoveInput>;
  try {
    const body = await readMutationBody(request);
    input = action === "create" ? parseSubscriptionCreateInput(body) : parseSubscriptionRemoveInput(body);
  } catch (error) {
    if (error instanceof MutationRequestError) { closeAfterMutationError(request, response, error.status, error.code); return; }
    const code = error instanceof MutationInputError ? error.code : "invalid_mutation";
    sendJson(response, 400, { error: code }); return;
  }
  try {
    const command = action === "create" ? "subscriptions.create" : "subscriptions.remove";
    const result = await sendMutationCommand(command, input);
    if (result.status >= 400) {
      if ([400, 404, 409, 422, 429].includes(result.status)) {
        const code = coreFaultCode(result.value, result.status) ?? "core_rejected";
        sendJson(response, result.status, { status: "rejected", code, id: input.id }); return;
      }
      throw new MutationRequestError(502, "mutation_unconfirmed");
    }
    if (result.status !== 200) throw new MutationRequestError(502, "mutation_unconfirmed");
    if (action === "create") {
      const relation = subscriptionSnapshot(result.value, input as ReturnType<typeof parseSubscriptionCreateInput>);
      sendJson(response, 200, { status: "applied", action, subscription: relation }); return;
    }
    const removed = removedSubscription(result.value, input as ReturnType<typeof parseSubscriptionRemoveInput>);
    sendJson(response, 200, { status: "applied", action, ...removed });
  } catch (error) {
    const failure = error instanceof MutationRequestError ? error : new MutationRequestError(502, "mutation_unconfirmed");
    const id = input.id;
    sendJson(response, failure.status, { status: "unconfirmed", code: "mutation_unconfirmed", id, action });
  }
}

function abortSnapshot(value: unknown, expected: ReturnType<typeof parseCallAbortInput>): SafeCall {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("response_invalid");
  const snapshot = value as Record<string, unknown>;
  const subject = snapshot.subject as Record<string, unknown> | null;
  if (!subject || subject.namespace !== "calls" || subject.id !== expected.id ||
      typeof snapshot.revision !== "number" || !Number.isSafeInteger(snapshot.revision) || snapshot.revision < expected.expected_revision) throw new Error("response_invalid");
  const rawValue = snapshot.value as Record<string, unknown> | null;
  const rawCall = rawValue?.call as Record<string, unknown> | null;
  if (!rawCall || rawCall.id !== expected.id || typeof rawValue?.abort_requested !== "boolean") throw new Error("response_invalid");
  const projected = projectCall(value);
  if (projected.subject.namespace !== "calls" || projected.subject.id !== expected.id || projected.revision < expected.expected_revision) throw new Error("response_invalid");
  return projected;
}

async function abortCall(request: IncomingMessage, response: ServerResponse): Promise<void> {
  if (request.url?.includes("?")) { sendJson(response, 400, { error: "invalid_query" }); return; }
  if (request.headers.origin === undefined) { sendJson(response, 403, { error: "local_origin_required" }); return; }
  if (!validateMutationContentType(request.headers["content-type"])) { sendJson(response, 415, { error: "content_type_required" }); return; }
  const length = request.headers["content-length"];
  if (length !== undefined && (!/^[0-9]+$/u.test(length) || Number(length) > MAX_ABORT_BODY)) { closeAfterMutationError(request, response, 413, "body_too_large"); return; }
  let input: ReturnType<typeof parseCallAbortInput>;
  try { input = parseCallAbortInput(await readMutationBody(request, MAX_ABORT_BODY)); }
  catch (error) {
    if (error instanceof MutationRequestError) { closeAfterMutationError(request, response, error.status, error.code); return; }
    sendJson(response, 400, { error: error instanceof MutationInputError ? error.code : "invalid_mutation" }); return;
  }
  try {
    const result = await sendMutationCommand("calls.abort", { ...input, reason: "operator_intervention" });
    if (result.status >= 400) {
      if ([400, 403, 404, 409, 422, 429].includes(result.status)) {
        const code = coreFaultCode(result.value, result.status) ?? "core_rejected";
        sendJson(response, result.status, { status: "rejected", code, id: input.id, action: "abort" }); return;
      }
      throw new MutationRequestError(502, "mutation_unconfirmed");
    }
    if (result.status !== 200) throw new MutationRequestError(502, "mutation_unconfirmed");
    const call = abortSnapshot(result.value, input);
    let latest: ReturnType<typeof projectCore>;
    try { latest = projectCore(await callCore("core.status", {})); }
    catch (error) { throw new MutationRequestError(error instanceof Error && error.name === "TimeoutError" ? 504 : 502, "mutation_unconfirmed"); }
    if (latest.instance_id !== input.expected_instance_id) throw new MutationRequestError(502, "mutation_unconfirmed");
    sendJson(response, 200, { status: "confirmed", id: input.id, action: "abort", call });
  } catch (error) {
    const failure = error instanceof MutationRequestError ? error : new MutationRequestError(502, "mutation_unconfirmed");
    sendJson(response, failure.status, { status: "unconfirmed", code: "mutation_unconfirmed", id: input.id, action: "abort" });
  }
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
    requireSameOriginFetchSite: request.method === "POST" && ["/api/subscriptions", "/api/subscriptions/remove", "/api/calls/abort"].includes(url.pathname),
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
  if (request.method === "POST" && url.pathname === "/api/subscriptions") {
    await mutateSubscription(request, response, "create"); return;
  }
  if (request.method === "POST" && url.pathname === "/api/subscriptions/remove") {
    await mutateSubscription(request, response, "remove"); return;
  }
  if (request.method === "POST" && url.pathname === "/api/calls/abort") {
    await abortCall(request, response); return;
  }
  if (request.method !== "GET" || url.pathname.startsWith("/api/")) {
    sendJson(response, 404, { error: "not_found" }); return;
  }
  await serveStatic(url.pathname, response);
});

server.listen(PORT, "127.0.0.1");
