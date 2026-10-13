import test from "node:test";
import assert from "node:assert/strict";
import {
  CALL_STATES, confirmedCoreDisplayState, continuousCallTransitionPairs, coreInstanceChanged, hasContinuousJournal, keepHighestCallRevisions, mergeJournalEvidence, parseCallsQuery, parseListLimit, parseLogsQuery, projectCall, projectCalls, projectConversations, projectCore, projectLogs, projectNodeDetails, projectNodes, projectRelations, type SafeCall,
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
  assert.equal(validDashboardRequest({ ...good, origin: undefined, fetchSite: undefined }), false);
});
