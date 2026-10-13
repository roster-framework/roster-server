export const CALL_STATES = [
  "created",
  "validating",
  "rejected",
  "queued",
  "preparing",
  "dispatching",
  "accepted",
  "running",
  "waiting",
  "resuming",
  "cancelling",
  "unknown",
  "reconciling",
  "succeeded",
  "failed",
  "cancelled",
  "expired",
] as const;

export type CallState = (typeof CALL_STATES)[number];

export interface CallsQuery {
  node_id?: string;
  correlation_id?: string;
  state?: CallState;
  limit: number;
}

export interface SafeLifecycle {
  state: CallState;
  updated_at: string;
}

export interface SafeCore {
  instance_id: string;
  state: CallState;
  updated_at: string;
  ready: boolean;
  local_stopped: boolean;
  unresolved_call_ids: string[];
  pending_delivery_count: number;
  pending_observation_count: number;
}

export interface SafeNode {
  id: string;
  connector_id: string;
}

export interface SafeCall {
  subject: { namespace: string; id: string };
  revision: number;
  call: {
    id: string;
    target: { namespace: string; id: string };
    operation: string;
    correlation_id?: string;
    causation_id?: string;
    conversation_id?: string;
    deadline?: string;
  };
  lifecycle: SafeLifecycle;
  abort_requested: boolean;
  result: { state: CallState; error_present: boolean } | null;
}

export interface ApiMeta {
  fetched_at: string;
  stale: boolean;
  returned_count: number;
  truncated: boolean;
  errors: Record<string, string>;
}

export interface OverviewSource {
  attempted_at: string;
  error: string | null;
  returned_count: number;
  truncated: boolean;
}

export interface OverviewResponse {
  meta: ApiMeta;
  sources: { core: OverviewSource; nodes: OverviewSource; calls: OverviewSource };
  core: SafeCore | null;
  nodes: SafeNode[] | null;
  calls: SafeCall[] | null;
}

export interface CallsResponse {
  meta: ApiMeta;
  calls: SafeCall[] | null;
}

export type CoreDisplayState = "ready" | "stopped" | "not_ready" | "unconfirmed" | "connecting";

export function confirmedCoreDisplayState(
  overview: OverviewResponse | null,
  resourceStale: boolean,
  refreshError: string | null,
): CoreDisplayState {
  if (!overview) return refreshError ? "unconfirmed" : "connecting";
  if (resourceStale || refreshError || overview.meta.stale || overview.sources.core.error || !overview.core) return "unconfirmed";
  if (overview.core.ready) return "ready";
  return overview.core.local_stopped ? "stopped" : "not_ready";
}

export function coreInstanceChanged(previous: string | null, current: string | null): boolean {
  return Boolean(previous && current && previous !== current);
}

export function keepHighestCallRevisions<T>(previous: T | null, incoming: T, allowRevisionMerge = true): T {
  if (!previous || typeof previous !== "object" || typeof incoming !== "object" || previous === null || incoming === null) return incoming;
  const oldData = previous as Record<string, unknown>;
  const newData = { ...(incoming as object) } as Record<string, unknown>;
  const oldInstance = (oldData.core as { instance_id?: unknown } | null)?.instance_id;
  const newInstance = (newData.core as { instance_id?: unknown } | null)?.instance_id;
  const hasCoreIdentity = Object.prototype.hasOwnProperty.call(oldData, "core") || Object.prototype.hasOwnProperty.call(newData, "core");
  const sameConfirmedInstance = typeof oldInstance === "string" && typeof newInstance === "string" && oldInstance === newInstance;
  if (typeof oldInstance === "string" && typeof newInstance === "string" && oldInstance !== newInstance) return incoming;
  const retained: string[] = [];
  for (const key of ["core", "nodes", "calls"]) {
    if (newData[key] === null && oldData[key] !== null && oldData[key] !== undefined) {
      newData[key] = oldData[key];
      retained.push(key);
    }
  }
  const nextMeta = newData.meta as Record<string, unknown> | undefined;
  const oldMeta = oldData.meta as Record<string, unknown> | undefined;
  const nextSources = newData.sources as Record<string, unknown> | undefined;
  const oldSources = oldData.sources as Record<string, unknown> | undefined;
  if (nextSources && oldSources && nextMeta) {
    const mergedSources = { ...nextSources };
    for (const key of retained.filter((item) => item === "nodes" || item === "calls")) {
      const current = nextSources[key];
      const previousSource = oldSources[key];
      if (!current || typeof current !== "object" || !previousSource || typeof previousSource !== "object") continue;
      mergedSources[key] = {
        ...(previousSource as Record<string, unknown>),
        ...(current as Record<string, unknown>),
        returned_count: (previousSource as Record<string, unknown>).returned_count,
        truncated: (previousSource as Record<string, unknown>).truncated,
      };
    }
    newData.sources = mergedSources;
    const nodes = Array.isArray(newData.nodes) ? newData.nodes.length : 0;
    const calls = Array.isArray(newData.calls) ? newData.calls.length : 0;
    const nodeSource = mergedSources.nodes as Record<string, unknown> | undefined;
    const callSource = mergedSources.calls as Record<string, unknown> | undefined;
    newData.meta = {
      ...nextMeta,
      returned_count: nodes + calls,
      truncated: Boolean(nodeSource?.truncated || callSource?.truncated),
    };
  } else if (retained.includes("calls") && nextMeta && oldMeta) {
    newData.meta = { ...nextMeta, returned_count: oldMeta.returned_count, truncated: oldMeta.truncated };
  }
  if (!allowRevisionMerge || (hasCoreIdentity && !sameConfirmedInstance)) return newData as T;
  const oldCalls = oldData.calls;
  const newCalls = newData.calls;
  if (!Array.isArray(oldCalls) || !Array.isArray(newCalls)) return newData as T;
  const byId = new Map<string, { revision: number }>();
  for (const call of oldCalls) {
    if (call && typeof call === "object" && "call" in call && "revision" in call) {
      const item = call as { call?: { id?: unknown }; revision?: unknown };
      if (typeof item.call?.id === "string" && typeof item.revision === "number") byId.set(item.call.id, item as { revision: number });
    }
  }
  newData.calls = newCalls.map((call) => {
    if (!call || typeof call !== "object" || !("call" in call) || !("revision" in call)) return call;
    const item = call as { call?: { id?: unknown }; revision?: unknown };
    const old = typeof item.call?.id === "string" ? byId.get(item.call.id) : undefined;
    return old && typeof item.revision === "number" && old.revision > item.revision ? old : call;
  });
  return newData as T;
}

export class ProjectionError extends Error {
  constructor() {
    super("Core response did not match the dashboard projection contract");
    this.name = "ProjectionError";
  }
}

function record(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new ProjectionError();
  }
  return value as Record<string, unknown>;
}

function boundedText(value: unknown, max: number, allowEmpty = false): string {
  if (
    typeof value !== "string" ||
    value.length > max ||
    (!allowEmpty && value.length === 0) ||
    /[\u0000-\u001f\u007f]/u.test(value)
  ) {
    throw new ProjectionError();
  }
  return value;
}

function id(value: unknown): string {
  const parsed = boundedText(value, 100);
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$/u.test(parsed)) {
    throw new ProjectionError();
  }
  return parsed;
}

function operation(value: unknown): string {
  const parsed = boundedText(value, 128);
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u.test(parsed)) {
    throw new ProjectionError();
  }
  return parsed;
}

function state(value: unknown): CallState {
  if (typeof value !== "string" || !CALL_STATES.includes(value as CallState)) {
    throw new ProjectionError();
  }
  return value as CallState;
}

function revision(value: unknown): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new ProjectionError();
  }
  return value;
}

function timestamp(value: unknown): string {
  const parsed = boundedText(value, 40);
  if (!Number.isFinite(Date.parse(parsed))) throw new ProjectionError();
  return parsed;
}

function optionalId(source: Record<string, unknown>, key: string): string | undefined {
  return source[key] === undefined ? undefined : id(source[key]);
}

function safeLifecycle(value: unknown): SafeLifecycle {
  const lifecycle = record(value);
  return {
    state: state(lifecycle.state),
    updated_at: timestamp(lifecycle.updated_at),
  };
}

export function projectCore(value: unknown): SafeCore {
  const source = record(value);
  const lifecycle = record(source.lifecycle);
  const lifecycleValue = record(lifecycle.value);
  if (typeof source.ready !== "boolean" || typeof source.local_stopped !== "boolean") {
    throw new ProjectionError();
  }
  if (!Array.isArray(source.unresolved_call_ids)) throw new ProjectionError();
  const unresolved = source.unresolved_call_ids.map(id);
  const delivery = revision(source.pending_delivery_count);
  const observation = revision(source.pending_observation_count);
  return {
    instance_id: id(source.instance_id),
    state: state(lifecycleValue.state),
    updated_at: timestamp(lifecycleValue.updated_at),
    ready: source.ready,
    local_stopped: source.local_stopped,
    unresolved_call_ids: unresolved,
    pending_delivery_count: delivery,
    pending_observation_count: observation,
  };
}

export function projectNodes(value: unknown, limit = Number.MAX_SAFE_INTEGER): SafeNode[] {
  if (!Array.isArray(value)) throw new ProjectionError();
  return value
    .map((entry) => {
      const node = record(entry);
      return { id: id(node.id), connector_id: id(node.connector_id) };
    })
    .sort((left, right) => left.id < right.id ? -1 : left.id > right.id ? 1 : 0)
    .slice(0, limit);
}

export function projectCall(value: unknown): SafeCall {
  const snapshot = record(value);
  const subjectValue = record(snapshot.subject);
  const execution = record(snapshot.value);
  const callValue = record(execution.call);
  const targetValue = record(callValue.target);
  const subject = {
    namespace: id(subjectValue.namespace),
    id: id(subjectValue.id),
  };
  const target = {
    namespace: id(targetValue.namespace),
    id: id(targetValue.id),
  };
  const call: SafeCall["call"] = {
    id: id(callValue.id),
    target,
    operation: operation(callValue.operation),
  };
  const correlation = optionalId(callValue, "correlation_id");
  const causation = optionalId(callValue, "causation_id");
  const conversation = optionalId(callValue, "conversation_id");
  if (correlation !== undefined) call.correlation_id = correlation;
  if (causation !== undefined) call.causation_id = causation;
  if (conversation !== undefined) call.conversation_id = conversation;
  if (callValue.deadline !== undefined) call.deadline = timestamp(callValue.deadline);

  if (typeof execution.abort_requested !== "boolean") throw new ProjectionError();
  let result: SafeCall["result"] = null;
  if (execution.result !== undefined && execution.result !== null) {
    const resultValue = record(execution.result);
    let errorPresent = false;
    if (resultValue.error !== undefined && resultValue.error !== null) {
      const error = record(resultValue.error);
      boundedText(error.code, 64);
      errorPresent = true;
    }
    result = { state: state(resultValue.state), error_present: errorPresent };
  }

  return {
    subject,
    revision: revision(snapshot.revision),
    call,
    lifecycle: safeLifecycle(execution.lifecycle),
    abort_requested: execution.abort_requested,
    result,
  };
}

export function projectCalls(value: unknown, limit = 100): SafeCall[] {
  if (!Array.isArray(value)) throw new ProjectionError();
  return value
    .map(projectCall)
    .sort((left, right) => left.call.id < right.call.id ? -1 : left.call.id > right.call.id ? 1 : 0)
    .slice(0, limit);
}

export function parseCallsQuery(params: URLSearchParams): CallsQuery {
  const allowed = new Set(["node_id", "correlation_id", "state", "limit"]);
  const values = new Map<string, string>();
  for (const [key, value] of params) {
    if (!allowed.has(key) || values.has(key)) throw new Error("invalid_query");
    values.set(key, value);
  }
  const query: CallsQuery = { limit: 50 };
  const node = values.get("node_id");
  const correlation = values.get("correlation_id");
  const selectedState = values.get("state");
  const limit = values.get("limit");
  if (node !== undefined) query.node_id = id(node);
  if (correlation !== undefined) query.correlation_id = id(correlation);
  if (selectedState !== undefined) query.state = state(selectedState);
  if (limit !== undefined) {
    if (!/^[0-9]+$/u.test(limit)) throw new Error("invalid_query");
    const parsed = Number(limit);
    if (!Number.isSafeInteger(parsed) || parsed < 1 || parsed > 100) {
      throw new Error("invalid_query");
    }
    query.limit = parsed;
  }
  return query;
}

export function validDashboardRequest(input: {
  remoteAddress: string | undefined;
  host: string | undefined;
  origin: string | undefined;
  fetchSite: string | undefined;
  port: number;
}): boolean {
  const loopbackPeers = new Set(["127.0.0.1", "::1", "::ffff:127.0.0.1"]);
  if (!input.remoteAddress || !loopbackPeers.has(input.remoteAddress)) return false;
  if (!input.host) return false;
  const expectedHosts = new Set([`127.0.0.1:${input.port}`, `localhost:${input.port}`]);
  if (!expectedHosts.has(input.host.toLowerCase())) return false;
  if (input.fetchSite && !["same-origin", "none"].includes(input.fetchSite)) return false;
  if (input.origin) return input.origin === `http://${input.host}`;
  return input.fetchSite === "same-origin" || input.fetchSite === "none";
}
