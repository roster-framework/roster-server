import test from "node:test";
import assert from "node:assert/strict";
import { CLIENT_PROFILE, applyBundle, applyExitCode, coreOrigin, parseStrictJson, validateManifest, type BundlePlan } from "../src/bundle.js";
import { createServer } from "node:http";
import { spawn } from "node:child_process";
import {
  CALL_STATES, MutationInputError, confirmedCoreDisplayState, continuousCallTransitionPairs, coreInstanceChanged, hasContinuousJournal, keepHighestCallRevisions, mergeJournalEvidence, mutationResponseIsCurrent, subscriptionCanvasExtent, parseCallsQuery, parseListLimit, parseLogsQuery, parseSubscriptionCreateInput, parseSubscriptionRemoveInput, projectCall, projectCalls, projectConversations, projectCore, projectLogs, projectNodeDetails, projectNodes, projectRelations, type SafeCall,
  validDashboardRequest,
} from "../src/projection.js";

const callFixture = (id = "call-1") => ({
  subject: { namespace: "roster.execution", id }, revision: 4,
  value: { call: { id, target: { namespace: "roster.node", id: "node-a" }, operation: "render", input: { secret: "never project" }, correlation_id: "corr-1" },
    lifecycle: { state: "running", updated_at: "2026-10-13T00:00:00Z" },
    result: { state: "failed", output: { secret: "hidden" }, error: { code: "bad", message: "private detail" } },
    abort_requested: false, observation: { secret: "hidden" }, binding: { secret: "hidden" } },
});

test("call projection exposes only identifiers, lifecycle, and result presence", () => {
  const projected = projectCall(callFixture());
  const serialized = JSON.stringify(projected);
  assert.equal(projected.lifecycle.state, "running");
  assert.deepEqual(projected.result, { state: "failed", error_present: true });
  assert.equal(serialized.includes("never project"), false);
  assert.equal(serialized.includes("private detail"), false);
  assert.equal(serialized.includes("hidden"), false);
  assert.equal("input" in projected.call, false);
});

test("projection rejects malformed lifecycle and sorts calls by stable ID", () => {
  assert.throws(() => projectCall({ ...callFixture(), revision: -1 }));
  assert.deepEqual(projectCalls([callFixture("z"), callFixture("a")]).map((item) => item.call.id), ["a", "z"]);
  assert.throws(() => projectNodes([{ id: "node-a", connector_id: "../secret" }]));
  const nodes = Array.from({ length: 101 }, (_, index) => ({ id: `node-${index}`, connector_id: "connector-a" }));
  assert.equal(projectNodes(nodes, 100).length, 100);
});

test("Core projection retains status facts without private fields", () => {
  const core = projectCore({ instance_id: "core-1", lifecycle: { value: { state: "running", updated_at: "2026-10-13T00:00:00Z" } },
    ready: true, local_stopped: false, unresolved_call_ids: ["call-1"], pending_delivery_count: 2, pending_observation_count: 3, last_delivery_error: "hidden" });
  assert.equal(core.ready, true);
  assert.equal(core.pending_delivery_count, 2);
  assert.equal("last_delivery_error" in core, false);
});

test("stale or partially failed overview never confirms Core ready", () => {
  const overview = {
    meta: { fetched_at: "2026-10-13T00:00:00Z", stale: false, returned_count: 0, truncated: false, errors: {} },
    sources: { core: { attempted_at: "2026-10-13T00:00:00Z", error: null, returned_count: 1, truncated: false },
      nodes: { attempted_at: "2026-10-13T00:00:00Z", error: null, returned_count: 0, truncated: false },
      calls: { attempted_at: "2026-10-13T00:00:00Z", error: null, returned_count: 0, truncated: false } },
    core: { instance_id: "core-a", state: "running", updated_at: "2026-10-13T00:00:00Z", ready: true, local_stopped: false,
      unresolved_call_ids: [], pending_delivery_count: 0, pending_observation_count: 0 }, nodes: [], calls: [],
  } as const;
  assert.equal(confirmedCoreDisplayState(overview as never, false, null), "ready");
  assert.equal(confirmedCoreDisplayState({ ...overview, meta: { ...overview.meta, stale: true } } as never, false, null), "unconfirmed");
  assert.equal(confirmedCoreDisplayState({ ...overview, sources: { ...overview.sources, core: { ...overview.sources.core, error: "core_unavailable" } } } as never, false, null), "unconfirmed");
  assert.equal(confirmedCoreDisplayState(overview as never, true, null), "unconfirmed");
  assert.equal(coreInstanceChanged("core-a", "core-b"), true);
  assert.equal(coreInstanceChanged("core-a", null), false);
});

test("unknown Core identity does not retain an older call revision", () => {
  type MergeFixture = { core: { instance_id: string } | null; calls: SafeCall[]; meta: { returned_count: number; truncated: boolean } };
  const prior: MergeFixture = { core: { instance_id: "core-old" }, calls: [projectCall({ ...callFixture(), revision: 20 })], meta: { returned_count: 1, truncated: false } };
  const next: MergeFixture = { core: null, calls: [projectCall({ ...callFixture(), revision: 2 })], meta: { returned_count: 1, truncated: false } };
  const merged = keepHighestCallRevisions(prior, next);
  assert.equal(merged.calls[0]?.revision, 2);
  assert.equal(merged.core?.instance_id, "core-old");

  const priorOverview = { ...prior, nodes: [], meta: { returned_count: 8, truncated: true } };
  const partialOverview = { ...next, calls: [], nodes: [], meta: { returned_count: 0, truncated: false } };
  const partial = keepHighestCallRevisions(priorOverview, partialOverview);
  assert.equal(partial.calls.length, 0);
  assert.equal(partial.meta.returned_count, 0);
  assert.equal(partial.meta.truncated, false);

  type OverviewMergeFixture = { core: { instance_id: string } | null; calls: SafeCall[]; nodes: unknown[] | null; meta: { returned_count: number; truncated: boolean } };
  const sameCorePrior: OverviewMergeFixture = { ...priorOverview, core: { instance_id: "core-a" } };
  const sameCoreNext: OverviewMergeFixture = { ...partialOverview, core: { instance_id: "core-a" }, nodes: null };
  const sameCorePartial = keepHighestCallRevisions(sameCorePrior, sameCoreNext);
  assert.equal(sameCorePartial.calls.length, 0);
  assert.equal(sameCorePartial.meta.returned_count, 0);
  assert.equal(sameCorePartial.meta.truncated, false);

  const sameCoreLowerRevision = { ...next, core: { instance_id: "core-old" } };
  const sameCoreMerge = keepHighestCallRevisions(prior, sameCoreLowerRevision);
  assert.equal(sameCoreMerge.calls[0]?.revision, 20);

  type CallsMergeFixture = { calls: SafeCall[] | null; meta: { returned_count: number; truncated: boolean } };
  const callsOnlyPrior: CallsMergeFixture = { calls: prior.calls, meta: { returned_count: 1, truncated: true } };
  const callsOnlyNext: CallsMergeFixture = { calls: next.calls, meta: { returned_count: 1, truncated: false } };
  assert.equal(keepHighestCallRevisions(callsOnlyPrior, callsOnlyNext, false).calls?.[0]?.revision, 2);
  const unavailableCalls = keepHighestCallRevisions(callsOnlyPrior, { calls: null, meta: { returned_count: 0, truncated: false } }, false);
  assert.equal(unavailableCalls.calls?.[0]?.revision, 20);
  assert.equal(unavailableCalls.meta.returned_count, 1);
  assert.equal(unavailableCalls.meta.truncated, true);

  type OverviewSourceFixture = { attempted_at: string; error: string | null; returned_count: number; truncated: boolean };
  type RetainedOverviewFixture = {
    core: { instance_id: string } | null;
    nodes: { id: string; connector_id: string }[] | null;
    calls: SafeCall[] | null;
    sources: { core: OverviewSourceFixture; nodes: OverviewSourceFixture; calls: OverviewSourceFixture };
    meta: { fetched_at: string; stale: boolean; returned_count: number; truncated: boolean; errors: Record<string, string> };
  };
  const oldOverview: RetainedOverviewFixture = {
    core: { instance_id: "core-a" }, nodes: [{ id: "node-a", connector_id: "connector-a" }], calls: prior.calls,
    sources: { core: { attempted_at: "old", error: null, returned_count: 1, truncated: false },
      nodes: { attempted_at: "old", error: null, returned_count: 1, truncated: true },
      calls: { attempted_at: "old", error: null, returned_count: 1, truncated: false } },
    meta: { fetched_at: "old", stale: false, returned_count: 2, truncated: true, errors: {} },
  };
  const newPartial: RetainedOverviewFixture = {
    core: { instance_id: "core-a" }, nodes: null, calls: [],
    sources: { core: { attempted_at: "new", error: null, returned_count: 1, truncated: false },
      nodes: { attempted_at: "new", error: "core_unavailable", returned_count: 0, truncated: false },
      calls: { attempted_at: "new", error: null, returned_count: 0, truncated: false } },
    meta: { fetched_at: "new", stale: true, returned_count: 0, truncated: false, errors: { nodes: "core_unavailable" } },
  };
  const retainedPartial = keepHighestCallRevisions(oldOverview, newPartial);
  assert.equal(retainedPartial.nodes?.length, 1);
  assert.equal(retainedPartial.calls?.length, 0);
  assert.equal(retainedPartial.sources.nodes.attempted_at, "new");
  assert.equal(retainedPartial.sources.nodes.error, "core_unavailable");
  assert.equal(retainedPartial.sources.nodes.returned_count, 1);
  assert.equal(retainedPartial.sources.nodes.truncated, true);
  assert.equal(retainedPartial.meta.returned_count, 1);
  assert.equal(retainedPartial.meta.truncated, true);
  assert.equal(retainedPartial.meta.stale, true);
});

test("call query allowlist rejects duplicates, unknown fields, bad states, and excess limits", () => {
  assert.equal(parseCallsQuery(new URLSearchParams("limit=2&state=running")).limit, 2);
  for (const query of ["state=not-a-state", "limit=101", "node_id=a&node_id=b", "payload=x"]) {
    assert.throws(() => parseCallsQuery(new URLSearchParams(query)));
  }
  assert.ok(CALL_STATES.includes("unknown"));
});

test("relation projections expose only declared Node, subscription, and conversation fields", () => {
  const node = projectNodeDetails([{ id: "node-a", connector_id: "connector-a", binding: { secret: "hidden" },
    operations: { render: { input_schema: { secret: true }, capabilities: { abort: true, inspect: false, resume: false } } },
    events: { rendered: { schema_secret: true } } }])[0]!;
  assert.deepEqual(node.operations, [{ name: "render", capabilities: { abort: true, inspect: false, resume: false } }]);
  assert.deepEqual(node.events, ["rendered"]);
  assert.equal(JSON.stringify(node).includes("secret"), false);

  const relation = projectRelations([{ subject: { namespace: "subscriptions", id: "sub-a" }, revision: 3,
    value: { id: "sub-a", source: { namespace: "nodes", id: "node-a" }, event: "rendered",
      target: { namespace: "nodes", id: "node-b" }, operation: "consume", input: { secret: true } } }])[0]!;
  assert.equal(relation.removed, false);
  assert.equal(relation.revision, 3);
  assert.equal(JSON.stringify(relation).includes("secret"), false);

  const conversation = projectConversations([{ revision: 2, value: { id: "conversation-a", title: "private title",
    participants: [{ ref: { namespace: "nodes", id: "node-a" }, receiver: {
      target: { namespace: "nodes", id: "node-b" }, operation: "reply", input: { secret: true },
    } }] } }])[0]!;
  assert.equal(conversation.participants[0]?.receiver?.operation, "reply");
  assert.equal(JSON.stringify(conversation).includes("private"), false);
  assert.equal(JSON.stringify(conversation).includes("secret"), false);
});

test("subscription mutation input validates provider-neutral declarations and data-only mappings", () => {
  const request = parseSubscriptionCreateInput({ id: "sub-a", source: { namespace: "nodes", id: "node-a" }, event: "finished",
    target: { namespace: "nodes", id: "node-b" }, operation: "run", input: { values: { request: "explicit" }, pointers: { context: "/payload/summary" } } });
  assert.deepEqual(request.source, { namespace: "nodes", id: "node-a" });
  assert.deepEqual(request.input.values, { request: "explicit" });
  assert.deepEqual(Object.fromEntries(Object.entries(request.input.pointers ?? {})), { context: "/payload/summary" });
  assert.deepEqual(Object.fromEntries(Object.entries(parseSubscriptionCreateInput({ id: "sub-root", source: { namespace: "nodes", id: "a" }, event: "e", target: { namespace: "nodes", id: "b" }, operation: "op", input: { pointers: { value: "" } } }).input.pointers ?? {})), { value: "" });
  const invalid = [
    { ...request, unexpected: true },
    { ...request, source: { namespace: "runtime", id: "node-a" } },
    { ...request, input: { values: JSON.parse('{"__proto__":"unsafe"}') } },
    { ...request, input: { values: { x: { constructor: { polluted: true } } } } },
    { ...request, input: { values: { ["x".repeat(101)]: 1 } } },
    { ...request, input: { values: Object.fromEntries(Array.from({ length: 65 }, (_, index) => [`k${index}`, index])) } },
    { ...request, input: { pointers: { value: "/bad~2escape" } } },
    { ...request, input: { pointers: { value: "/__proto__/x" } } },
    { ...request, input: { values: { payload: "x".repeat(16 * 1024) } } },
  ];
  for (const value of invalid) assert.throws(() => parseSubscriptionCreateInput(value), MutationInputError);
  assert.throws(() => parseSubscriptionCreateInput({ ...request, removed: false }), MutationInputError);
});

test("subscription removal requires an observed positive safe revision", () => {
  assert.deepEqual(parseSubscriptionRemoveInput({ id: "sub-a", expected_revision: 4 }), { id: "sub-a", expected_revision: 4 });
  for (const value of [
    { id: "sub-a", expected_revision: 0 }, { id: "sub-a", expected_revision: Number.MAX_SAFE_INTEGER + 1 },
    { id: "sub-a", expected_revision: 4, extra: "ignored?" }, { id: "../sub-a", expected_revision: 4 },
  ]) assert.throws(() => parseSubscriptionRemoveInput(value), MutationInputError);
});

test("subscription mutation responses are ignored after generation or Core identity changes", () => {
  assert.equal(mutationResponseIsCurrent(7, 7, "core-a", "core-a"), true);
  assert.equal(mutationResponseIsCurrent(7, 8, "core-a", "core-a"), false);
  assert.equal(mutationResponseIsCurrent(7, 7, "core-a", "core-b"), false);
});

test("subscription canvas extent follows cards when wrapping narrow and shrinking wide", () => {
  const narrow = subscriptionCanvasExtent(600, 480, [{ right: 580, bottom: 700 }], 12, 18);
  assert.deepEqual(narrow, { width: 600, height: 718 });
  const wide = subscriptionCanvasExtent(900, 274, [{ right: 860, bottom: 242 }], 12, 18);
  assert.deepEqual(wide, { width: 900, height: 274 });
  const borderAligned = subscriptionCanvasExtent(982, 274, [{ right: 964.984, bottom: 257 }], 17, 17);
  assert.deepEqual(borderAligned, { width: 982, height: 274 });
});

test("gateway validates raw subscription receipts and returns only safe mutation outcomes", async () => {
  const commands: Array<{ path: string; body: Record<string, unknown> }> = [];
  let hangingResponse: import("node:http").ServerResponse | undefined;
  const core = createServer(async (request, response) => {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    const body = JSON.parse(Buffer.concat(chunks).toString("utf8")) as Record<string, unknown>;
    const path = request.url ?? "";
    if (path === "/api/commands/subscriptions.list") {
      response.writeHead(200, { "Content-Type": "application/json" }); response.end("[]"); return;
    }
    commands.push({ path, body });
    if (path.endsWith("subscriptions.create")) {
      const id = String(body.id);
      if (id === "sub-server-error") { response.writeHead(500, { "Content-Type": "application/json" }); response.end(JSON.stringify({ message: "private Core failure" })); return; }
      if (id === "sub-timeout") { hangingResponse = response; return; }
      if (id === "sub-unknown") { response.writeHead(400, { "Content-Type": "application/json" }); response.end(JSON.stringify({ code: "future_fault", message: "private" })); return; }
      if (id === "sub-schema") { response.writeHead(400, { "Content-Type": "application/json" }); response.end(JSON.stringify({ code: "invalid_schema", message: "private" })); return; }
      const value = { id, source: body.source, event: body.event, target: body.target, operation: body.operation,
        input: { values: { private_value: "must not echo" } }, ...(id === "sub-removed" ? { removed: true } : {}) };
      response.writeHead(200, { "Content-Type": "application/json" });
      response.end(JSON.stringify({ subject: { namespace: "subscriptions", id }, revision: 1, value })); return;
    }
    const id = String(body.id); const revision = Number(body.expected_revision);
    response.writeHead(200, { "Content-Type": "application/json" });
    response.end(JSON.stringify({ id: "event-change-id", subject: { namespace: "subscriptions", id }, before_revision: revision,
      after_revision: revision + 1, after: { id, source: { namespace: "nodes", id: "node-a" }, event: "changed",
        target: { namespace: "nodes", id: "node-b" }, operation: "apply", removed: true } }));
  });
  await new Promise<void>((resolve, reject) => core.listen(7350, "127.0.0.1", (error?: Error) => error ? reject(error) : resolve()));
  const probe = createServer();
  await new Promise<void>((resolve) => probe.listen(0, "127.0.0.1", resolve));
  const address = probe.address(); assert.ok(address && typeof address !== "string");
  await new Promise<void>((resolve) => probe.close(() => resolve()));
  const child = spawn(process.execPath, ["build/src/gateway.js"], { cwd: process.cwd(), env: { ...process.env, PORT: String(address.port) }, stdio: "ignore" });
  const origin = `http://127.0.0.1:${address.port}`;
  try {
    let ready = false;
    for (let attempt = 0; attempt < 50 && !ready; attempt++) {
      try { ready = (await fetch(`${origin}/api/relations`, { headers: { Origin: origin } })).status === 200; }
      catch { await new Promise((resolve) => setTimeout(resolve, 20)); }
    }
    assert.equal(ready, true);
    const post = (path: string, body: unknown) => fetch(`${origin}${path}`, { method: "POST", headers: { Origin: origin, "Content-Type": "application/json" }, body: JSON.stringify(body) });
    const declaration = { source: { namespace: "nodes", id: "node-a" }, event: "changed", target: { namespace: "nodes", id: "node-b" }, operation: "apply", input: {} };
    const denied = async (headers: Record<string, string>) => fetch(`${origin}/api/subscriptions`, { method: "POST", headers, body: JSON.stringify({ ...declaration, id: "sub-denied" }) });
    assert.equal((await denied({ "Content-Type": "application/json" })).status, 403);
    assert.equal((await denied({ Origin: "http://example.invalid", "Content-Type": "application/json" })).status, 403);
    assert.equal((await denied({ Origin: "null", "Content-Type": "application/json" })).status, 403);
    const none = await fetch(`${origin}/api/subscriptions`, { method: "POST", headers: { Origin: origin, "Sec-Fetch-Site": "none", "Content-Type": "application/json" }, body: JSON.stringify({ ...declaration, id: "sub-no-site" }) });
    assert.equal(none.status, 403);
    assert.equal((await denied({ Origin: origin, "Sec-Fetch-Site": "cross-site", "Content-Type": "application/json" })).status, 403);
    assert.equal((await fetch(`${origin}/api/subscriptions`, { method: "POST", headers: { Origin: origin, "Content-Type": "text/plain" }, body: JSON.stringify({ ...declaration, id: "sub-media" }) })).status, 415);
    const malformedBody = await fetch(`${origin}/api/subscriptions`, { method: "POST", headers: { Origin: origin, "Content-Type": "application/json" }, body: "{not-json" });
    assert.equal(malformedBody.status, 400);
    const duplicateBody = await fetch(`${origin}/api/subscriptions`, { method: "POST", headers: { Origin: origin, "Content-Type": "application/json" }, body: '{"id":"sub-dup","id":"sub-dup","source":{"namespace":"nodes","id":"node-a"},"event":"changed","target":{"namespace":"nodes","id":"node-b"},"operation":"apply","input":{}}' });
    assert.equal(duplicateBody.status, 400);
    const oversizedBody = await fetch(`${origin}/api/subscriptions`, { method: "POST", headers: { Origin: origin, "Content-Type": "application/json" }, body: " ".repeat(33 * 1024) });
    assert.equal(oversizedBody.status, 413);
    assert.equal(commands.length, 0);
    const good = await post("/api/subscriptions", { ...declaration, id: "sub-good" });
    assert.equal(good.status, 200);
    const goodBody = await good.text(); assert.equal(goodBody.includes("private_value"), false); assert.equal(goodBody.includes("must not echo"), false);
    const malformed = await post("/api/subscriptions", { ...declaration, id: "sub-removed" });
    assert.equal(malformed.status, 502); assert.deepEqual(await malformed.json(), { status: "unconfirmed", code: "mutation_unconfirmed", id: "sub-removed", action: "create" });
    const unknown = await post("/api/subscriptions", { ...declaration, id: "sub-unknown" });
    assert.equal(unknown.status, 400); assert.deepEqual(await unknown.json(), { status: "rejected", code: "core_rejected", id: "sub-unknown" });
    const schema = await post("/api/subscriptions", { ...declaration, id: "sub-schema" });
    assert.equal(schema.status, 400); assert.deepEqual(await schema.json(), { status: "rejected", code: "invalid_schema", id: "sub-schema" });
    const serverError = await post("/api/subscriptions", { ...declaration, id: "sub-server-error" });
    assert.equal(serverError.status, 502); assert.deepEqual(await serverError.json(), { status: "unconfirmed", code: "mutation_unconfirmed", id: "sub-server-error", action: "create" });
    const timedOut = await post("/api/subscriptions", { ...declaration, id: "sub-timeout" });
    assert.equal(timedOut.status, 504); assert.deepEqual(await timedOut.json(), { status: "unconfirmed", code: "mutation_unconfirmed", id: "sub-timeout", action: "create" });
    const removed = await post("/api/subscriptions/remove", { id: "sub-good", expected_revision: 2 });
    assert.deepEqual(await removed.json(), { status: "applied", action: "remove", id: "sub-good", revision: 3, removed: true });
    assert.equal(commands.length, 7);
  } finally {
    hangingResponse?.destroy();
    child.kill("SIGTERM");
    await new Promise<void>((resolve, reject) => core.close((error) => error ? reject(error) : resolve()));
  }
});

test("log projection keeps safe envelope, validates only state.changed evidence, and rejects unsafe queries", () => {
  const event = (id: string, payload: unknown, sequence: number) => ({
    sequence, event: { id, source: { namespace: "calls", id: "call-a" }, name: "state.changed",
      category: "observation", occurred_at: "2026-10-13T00:00:00Z", call_id: "call-a",
      payload, private_field: "never project" },
  });
  const page = projectLogs({ entries: [
    event("event-a", { state: "running", before_revision: 4, after_revision: 5, reason: "private" }, 5),
    event("event-b", { state: "running", before_revision: 5, after_revision: 8, reason: "private" }, 6),
    event("event-c", { state: "running", before_revision: Number.MAX_SAFE_INTEGER + 1, after_revision: Number.MAX_SAFE_INTEGER + 2 }, 7),
    { sequence: { dashboard_unsafe_integer: true, source: "9007199254740993" }, event: { id: "event-d", source: { namespace: "nodes", id: "node-a" },
      name: "registered", category: "lifecycle", occurred_at: "2026-10-13T00:00:00Z", payload: { secret: true } } },
  ], next_cursor: "opaque:cursor"}, 10);
  assert.deepEqual(page.entries[0]?.transition_observation, { state: "running", before_revision: 4, after_revision: 5 });
  assert.equal(page.entries[1]?.projection_error, "invalid_state_observation");
  assert.equal(page.entries[2]?.projection_error, "invalid_state_observation");
  assert.equal(page.entries[3]?.sequence, "9007199254740993");
  assert.equal(JSON.stringify(page).includes("private"), false);
  assert.equal(JSON.stringify(page).includes("secret"), false);
  assert.equal(page.next_cursor, "opaque:cursor");
  assert.equal(parseListLimit(new URLSearchParams("limit=100"), ["limit"], 50, 100), 100);
  for (const query of ["limit=101", "limit=2&limit=3", "unknown=x"]) assert.throws(() => parseListLimit(new URLSearchParams(query), ["limit"], 50, 100));
  assert.equal(parseLogsQuery(new URLSearchParams("call_id=call-a&limit=200")).call_id, "call-a");
  for (const query of ["call_id=x&call_id=y", "limit=201", "other=x", "after=" + "é".repeat(600)]) assert.throws(() => parseLogsQuery(new URLSearchParams(query)));
});

test("journal transition evidence rejects invalid, conflicting, gapped, expired, and partial records", () => {
  const event = (id: string, sequence: number | null, before: number, after: number, state = "running", callId = "call-a", sourceId = callId) => ({
    sequence, event: { id, source: { namespace: "calls", id: sourceId }, name: "state.changed", category: "observation",
      occurred_at: "2026-10-13T00:00:00Z", call_id: callId, payload: { state, before_revision: before, after_revision: after } },
  });
  const project = (...entries: ReturnType<typeof event>[]) => projectLogs({ next_cursor: "", entries }).entries;
  const valid = project(event("event-1", 1, 1, 2), event("event-2", 2, 2, 3));
  assert.equal(continuousCallTransitionPairs(valid, "call-a", true).length, 1);
  assert.equal(continuousCallTransitionPairs(valid, "call-a", false).length, 0);
  assert.equal(continuousCallTransitionPairs(valid, "call-a", true, ["event-1"]).length, 0);
  assert.equal(hasContinuousJournal(project(event("event-1", 1, 1, 2), event("event-2", 3, 2, 3))), false);
  assert.equal(continuousCallTransitionPairs(project(event("event-1", 1, 1, 2), event("event-2", 3, 2, 3)), "call-a", true).length, 0);
  const invalidBreak = project(event("event-1", 1, 1, 2), event("event-bad", 2, 2, 4, "running", "other-call", "call-a"), event("event-2", 3, 2, 3));
  assert.equal(continuousCallTransitionPairs(invalidBreak, "call-a", true).length, 0);
  const repeatedRevision = project(event("event-1", 1, 1, 2), event("event-2", 2, 1, 2));
  assert.equal(continuousCallTransitionPairs(repeatedRevision, "call-a", true).length, 0);
  const ambiguousRevision = project(event("a", 1, 4, 5, "running"), event("b", 2, 4, 5, "failed"), event("c", 3, 5, 6, "waiting"));
  assert.equal(continuousCallTransitionPairs(ambiguousRevision, "call-a", true).length, 0);
  const unknownSequence = project(event("a", 1, 4, 5), event("unknown", null, 5, 6), event("c", 3, 6, 7));
  assert.equal(continuousCallTransitionPairs(unknownSequence, "call-a", true).length, 0);
  const rollingWindow = project(event("older", 800, 0, 1), event("event-1", 801, 1, 2), event("event-2", 802, 2, 3));
  assert.equal(continuousCallTransitionPairs(rollingWindow.slice(1), "call-a", true).length, 1);
  const conflicting = mergeJournalEvidence(valid, [{ ...valid[0]!, event: { ...valid[0]!.event, name: "different" } }]);
  assert.deepEqual(conflicting.conflictIds, ["event-1"]);
  assert.equal(continuousCallTransitionPairs(valid, "call-a", true, conflicting.conflictIds).length, 0);
});

test("gateway request guard requires a loopback peer and same-origin request", () => {
  const good = { remoteAddress: "127.0.0.1", host: "127.0.0.1:7360", origin: "http://127.0.0.1:7360", fetchSite: "same-origin", port: 7360 };
  assert.equal(validDashboardRequest(good), true);
  assert.equal(validDashboardRequest({ ...good, remoteAddress: "203.0.113.9" }), false);
  assert.equal(validDashboardRequest({ ...good, origin: "https://evil.invalid" }), false);
  assert.equal(validDashboardRequest({ ...good, host: "[invalid" }), false);
  assert.equal(validDashboardRequest({ ...good, fetchSite: "cross-site" }), false);
  assert.equal(validDashboardRequest({ ...good, fetchSite: "none", requireSameOriginFetchSite: true }), false);
  assert.equal(validDashboardRequest({ ...good, origin: undefined, fetchSite: undefined }), false);
});

test("bundle validation accepts only declarative references and never resolves token values", () => {
  const pkg = { name: "@sample/echo", version: "1.2.3", roster: { manifest: "roster-bundle.json" }, scripts: { postinstall: "must-not-run" } };
  const manifest = { schema: "roster-bundle/1", name: "@sample/echo", version: "1.2.3", client_profile: CLIENT_PROFILE,
    nodes: [{ id: "alpha", connector_id: "runtime", binding_ref: "alpha-binding" }, { id: "beta", connector_id: "runtime", binding_ref: "beta-binding" }],
    subscriptions: [{ id: "alpha-to-beta", source: { namespace: "nodes", id: "alpha" }, event: "finished", target: { namespace: "nodes", id: "beta" }, operation: "run", input: { values: { request: "hello" } } }] };
  const bindings = { bindings: {
    "alpha-binding": { url: "http://127.0.0.1:7331", target_id: "agent-a", token_env: "RUNTIME_TOKEN" },
    "beta-binding": { url: "https://runtime.example", target_id: "agent-b", token_env: "RUNTIME_TOKEN", watch: true },
  } };
  const accepted = validateManifest(pkg, manifest, bindings);
  assert.equal(accepted.packageInfo.name, "@sample/echo");
  assert.deepEqual(Object.keys(accepted.bindings), ["alpha-binding", "beta-binding"]);
  assert.equal(JSON.stringify(accepted).includes("RUNTIME_TOKEN_VALUE"), false);
  assert.equal(CLIENT_PROFILE, "roster-core-import/1");
  for (const changed of [
    { ...manifest, schema: "roster-bundle/2" },
    { ...manifest, unknown: true },
    { ...manifest, name: "other" },
    { ...manifest, version: "^1.2.3" },
    { ...manifest, subscriptions: [{ ...manifest.subscriptions[0], source: { namespace: "foreign", id: "alpha" } }] },
    { ...manifest, subscriptions: [{ ...manifest.subscriptions[0], source: { namespace: "nodes", id: "missing" } }] },
  ]) assert.throws(() => validateManifest(pkg, changed, bindings));
  assert.throws(() => validateManifest(pkg, manifest, { bindings: { "alpha-binding": { ...bindings.bindings["alpha-binding"], token: "secret" }, "beta-binding": bindings.bindings["beta-binding"] } }));
  assert.throws(() => validateManifest(pkg, manifest, { bindings: { "alpha-binding": { ...bindings.bindings["alpha-binding"], url: "http://provider.example" }, "beta-binding": bindings.bindings["beta-binding"] } }));
});

test("bundle CLI parser primitives reject duplicate JSON keys and non-loopback Core origins", () => {
  assert.throws(() => parseStrictJson('{"name":"first","name":"second"}'));
  assert.throws(() => parseStrictJson('{"a":[1,]}'));
  assert.equal(coreOrigin("http://127.0.0.1:7350"), "http://127.0.0.1:7350");
  assert.equal(coreOrigin("http://[::1]:7350"), "http://[::1]:7350");
  for (const origin of ["https://127.0.0.1:7350", "http://example.com", "http://127.0.0.1:7350/path", "http://u:p@127.0.0.1:7350"]) assert.throws(() => coreOrigin(origin));
});

test("bundle CLI maps pre-apply communication failure and partial mutation to distinct exits", () => {
  const result = (status: "planned" | "applied" | "partial" | "failed") => ({
    schema: "roster-import-result/1" as const, status, package: { name: "example", version: "1.0.0" },
    client_profile: CLIENT_PROFILE, digest: null, actions: [], applied: [], unresolved: [], failure: null,
  });
  assert.equal(applyExitCode(result("planned")), 0);
  assert.equal(applyExitCode(result("applied")), 0);
  assert.equal(applyExitCode(result("partial")), 3);
  assert.equal(applyExitCode(result("failed")), 4);
});

test("bundle apply records only identity-matching Core command responses", async () => {
  const server = createServer(async (request, response) => {
    for await (const _chunk of request) { /* consume request body */ }
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ id: "other-node", connector_id: "connector-a" }));
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const address = server.address();
    assert.ok(address && typeof address !== "string");
    const plan: BundlePlan = {
      package: { name: "example-bundle", version: "1.0.0" },
      manifest: { schema: "roster-bundle/1", name: "example-bundle", version: "1.0.0", client_profile: CLIENT_PROFILE, nodes: [], subscriptions: [] },
      bindings: {}, digest: "digest",
      actions: [{ command: "nodes.register", id: "wanted-node", input: { id: "wanted-node", connector_id: "connector-a" } }],
    };
    const result = await applyBundle(plan, `http://127.0.0.1:${address.port}`, Date.now() + 5_000);
    assert.equal(result.status, "partial");
    assert.deepEqual(result.applied, []);
    assert.deepEqual(result.unresolved, [{ command: "nodes.register", id: "wanted-node" }]);
    assert.deepEqual(result.failure, { stage: "apply", code: "core_response_invalid", id: "wanted-node" });
  } finally {
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }
});

test("bundle apply does not report an unsent expired mutation as unresolved", async () => {
  let requests = 0;
  const server = createServer((_request, response) => { requests++; response.writeHead(200); response.end("{}"); });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const address = server.address();
    assert.ok(address && typeof address !== "string");
    const plan: BundlePlan = {
      package: { name: "example-bundle", version: "1.0.0" },
      manifest: { schema: "roster-bundle/1", name: "example-bundle", version: "1.0.0", client_profile: CLIENT_PROFILE, nodes: [], subscriptions: [] },
      bindings: {}, digest: "digest",
      actions: [{ command: "nodes.register", id: "wanted-node", input: { id: "wanted-node", connector_id: "connector-a" } }],
    };
    const result = await applyBundle(plan, `http://127.0.0.1:${address.port}`, Date.now() - 1);
    assert.equal(result.status, "failed");
    assert.deepEqual(result.applied, []);
    assert.deepEqual(result.unresolved, []);
    assert.equal(requests, 0);
    assert.deepEqual(result.failure, { stage: "apply", code: "overall_timeout", id: "wanted-node" });
  } finally {
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }
});

test("bundle apply cancels an oversized Core response and keeps the action unresolved", async () => {
  let responseClosed = false;
  const server = createServer((_request, response) => {
    response.writeHead(200, { "content-type": "application/json" });
    response.on("close", () => { responseClosed = true; });
    response.write(Buffer.alloc(2 * 1024 * 1024 + 1, 0x20));
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const address = server.address();
    assert.ok(address && typeof address !== "string");
    const plan: BundlePlan = {
      package: { name: "example-bundle", version: "1.0.0" },
      manifest: { schema: "roster-bundle/1", name: "example-bundle", version: "1.0.0", client_profile: CLIENT_PROFILE, nodes: [], subscriptions: [] },
      bindings: {}, digest: "digest",
      actions: [{ command: "nodes.register", id: "wanted-node", input: { id: "wanted-node", connector_id: "connector-a" } }],
    };
    const result = await applyBundle(plan, `http://127.0.0.1:${address.port}`, Date.now() + 5_000);
    assert.equal(result.status, "partial");
    assert.deepEqual(result.unresolved, [{ command: "nodes.register", id: "wanted-node" }]);
    assert.deepEqual(result.failure, { stage: "apply", code: "core_response_invalid", id: "wanted-node" });
    for (let attempt = 0; attempt < 20 && !responseClosed; attempt++) await new Promise((resolve) => setTimeout(resolve, 10));
    assert.equal(responseClosed, true);
  } finally {
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }
});

test("bundle apply cancels redirect and error response bodies", async () => {
  const statuses = [302, 400, 500];
  const closed = statuses.map(() => false);
  let requestIndex = 0;
  const server = createServer((_request, response) => {
    const index = requestIndex++;
    response.writeHead(statuses[index]!);
    response.on("close", () => { closed[index] = true; });
    response.write(Buffer.alloc(64 * 1024, 0x20));
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const address = server.address();
    assert.ok(address && typeof address !== "string");
    const plan: BundlePlan = {
      package: { name: "example-bundle", version: "1.0.0" },
      manifest: { schema: "roster-bundle/1", name: "example-bundle", version: "1.0.0", client_profile: CLIENT_PROFILE, nodes: [], subscriptions: [] },
      bindings: {}, digest: "digest",
      actions: [{ command: "nodes.register", id: "wanted-node", input: { id: "wanted-node", connector_id: "connector-a" } }],
    };
    const expected = ["redirect_forbidden", "core_rejected", "core_unavailable"];
    for (let index = 0; index < statuses.length; index++) {
      const result = await applyBundle(plan, `http://127.0.0.1:${address.port}`, Date.now() + 5_000);
      assert.equal(result.status, "partial");
      assert.equal(result.failure?.code, expected[index]);
      assert.deepEqual(result.unresolved, index === 2 ? [{ command: "nodes.register", id: "wanted-node" }] : []);
      for (let attempt = 0; attempt < 20 && !closed[index]; attempt++) await new Promise((resolve) => setTimeout(resolve, 10));
      assert.equal(closed[index], true);
    }
  } finally {
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }
});
