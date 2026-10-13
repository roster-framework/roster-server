import test from "node:test";
import assert from "node:assert/strict";
import {
  CALL_STATES, confirmedCoreDisplayState, coreInstanceChanged, keepHighestCallRevisions, parseCallsQuery, projectCall, projectCalls, projectCore, projectNodes, type SafeCall,
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

test("gateway request guard requires a loopback peer and same-origin request", () => {
  const good = { remoteAddress: "127.0.0.1", host: "127.0.0.1:7360", origin: "http://127.0.0.1:7360", fetchSite: "same-origin", port: 7360 };
  assert.equal(validDashboardRequest(good), true);
  assert.equal(validDashboardRequest({ ...good, remoteAddress: "203.0.113.9" }), false);
  assert.equal(validDashboardRequest({ ...good, origin: "https://evil.invalid" }), false);
  assert.equal(validDashboardRequest({ ...good, host: "[invalid" }), false);
  assert.equal(validDashboardRequest({ ...good, fetchSite: "cross-site" }), false);
  assert.equal(validDashboardRequest({ ...good, origin: undefined, fetchSite: undefined }), false);
});
