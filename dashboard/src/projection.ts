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

export interface SafeNodeDetail extends SafeNode {
  operations: { name: string; capabilities: { abort: boolean; inspect: boolean; resume: boolean } }[];
  events: string[];
}

export interface SafeRef { namespace: string; id: string }
export interface SafeRelation {
  id: string; revision: number; removed: boolean; source: SafeRef; event: string;
  target: SafeRef; operation: string;
}
export interface SubscriptionCreateInput {
  id: string; source: { namespace: "nodes"; id: string }; event: string;
  target: { namespace: "nodes"; id: string }; operation: string;
  input: { values?: Record<string, unknown>; pointers?: Record<string, string> };
}
export interface SubscriptionRemoveInput { id: string; expected_revision: number }
export class MutationInputError extends Error { constructor(readonly code: string) { super(code); } }
export interface SafeConversation {
  id: string; revision: number; closed: boolean;
  participants: { ref: SafeRef; receiver?: { target: SafeRef; operation: string } }[];
}
export interface SafeLogEntry {
  sequence: number | string | null;
  event: { id: string; source: SafeRef; name: string; category: string; occurred_at: string;
    call_id?: string; correlation_id?: string; causation_id?: string; conversation_id?: string };
  transition_observation?: { state: CallState; before_revision: number; after_revision: number };
  projection_error?: "invalid_state_observation";
}
export interface ListResponse<T> { meta: ApiMeta; items: T[] | null }
export interface LogsResponse { meta: ApiMeta; entries: SafeLogEntry[] | null; next_cursor: string; cursor_expired: boolean }
export interface JournalEvidence { entries: SafeLogEntry[]; conflictIds: string[]; truncated: boolean }

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

const dangerousMappingKeys = new Set(["__proto__", "constructor", "prototype"]);
function mutationId(value: unknown): string {
  if (typeof value !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$/u.test(value)) throw new MutationInputError("invalid_mutation");
  return value;
}
function mutationRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new MutationInputError("invalid_mutation");
  return value as Record<string, unknown>;
}
function mutationKeys(value: Record<string, unknown>, required: string[], optional: string[] = []): void {
  const allowed = new Set([...required, ...optional]);
  if (Object.keys(value).some((key) => !allowed.has(key)) || required.some((key) => !Object.prototype.hasOwnProperty.call(value, key))) throw new MutationInputError("invalid_mutation");
}
function safeMappingValue(value: unknown): boolean {
  const stack: unknown[] = [value];
  while (stack.length) {
    const item = stack.pop();
    if (item === null || typeof item === "string" || typeof item === "boolean") continue;
    if (typeof item === "number") { if (!Number.isFinite(item)) return false; continue; }
    if (Array.isArray(item)) { stack.push(...item); continue; }
    if (typeof item !== "object") return false;
    for (const [key, child] of Object.entries(item)) {
      if (dangerousMappingKeys.has(key)) return false;
      stack.push(child);
    }
  }
  return true;
}
function mappingKeys(value: Record<string, unknown>): void {
  const keys = Object.keys(value);
  if (keys.length > 64 || keys.some((key) => dangerousMappingKeys.has(key) || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$/u.test(key))) throw new MutationInputError("invalid_mutation");
}
function validPointer(value: string): boolean {
  if (new TextEncoder().encode(value).byteLength > 1024 || (value !== "" && !value.startsWith("/")) || /~(?![01])/u.test(value)) return false;
  return !value.split("/").slice(1).some((segment) => dangerousMappingKeys.has(segment.replace(/~1/gu, "/").replace(/~0/gu, "~")));
}
export function parseSubscriptionCreateInput(value: unknown): SubscriptionCreateInput {
  try {
    const row = mutationRecord(value); mutationKeys(row, ["id", "source", "event", "target", "operation", "input"]);
    const source = mutationRecord(row.source); mutationKeys(source, ["namespace", "id"]);
    const target = mutationRecord(row.target); mutationKeys(target, ["namespace", "id"]);
    if (source.namespace !== "nodes" || target.namespace !== "nodes") throw new MutationInputError("invalid_mutation");
    const input = mutationRecord(row.input); mutationKeys(input, [], ["values", "pointers"]);
    const parsed: SubscriptionCreateInput = {
      id: mutationId(row.id), source: { namespace: "nodes", id: mutationId(source.id) }, event: mutationId(row.event),
      target: { namespace: "nodes", id: mutationId(target.id) }, operation: mutationId(row.operation), input: {},
    };
    if (input.values !== undefined) {
      const values = mutationRecord(input.values); mappingKeys(values);
      if (!safeMappingValue(values)) throw new MutationInputError("invalid_mutation");
      parsed.input.values = values;
    }
    if (input.pointers !== undefined) {
      const pointers = mutationRecord(input.pointers); mappingKeys(pointers);
      const mapped: Record<string, string> = Object.create(null) as Record<string, string>;
      for (const [key, pointer] of Object.entries(pointers)) {
        if (typeof pointer !== "string" || !validPointer(pointer)) throw new MutationInputError("invalid_mutation");
        mapped[key] = pointer;
      }
      parsed.input.pointers = mapped;
    }
    if (new TextEncoder().encode(JSON.stringify(parsed.input)).byteLength > 16 * 1024) throw new MutationInputError("invalid_mutation");
    return parsed;
  } catch (error) {
    if (error instanceof MutationInputError) throw error;
    throw new MutationInputError("invalid_mutation");
  }
}
export function parseSubscriptionRemoveInput(value: unknown): SubscriptionRemoveInput {
  const row = mutationRecord(value); mutationKeys(row, ["id", "expected_revision"]);
  if (typeof row.expected_revision !== "number" || !Number.isSafeInteger(row.expected_revision) || row.expected_revision <= 0) throw new MutationInputError("invalid_mutation");
  return { id: mutationId(row.id), expected_revision: row.expected_revision };
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

function sequence(value: unknown): number | string | null {
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) return value;
  if (value !== null && typeof value === "object" && !Array.isArray(value)) {
    const unsafe = value as Record<string, unknown>;
    if (unsafe.dashboard_unsafe_integer === true && typeof unsafe.source === "string" && /^(0|[1-9][0-9]*)$/u.test(unsafe.source)) return unsafe.source;
  }
  return null;
}

function ref(value: unknown): SafeRef {
  const source = record(value);
  return { namespace: id(source.namespace), id: id(source.id) };
}

function boolean(value: unknown): boolean {
  if (typeof value !== "boolean") throw new ProjectionError();
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

export function projectNodeDetails(value: unknown, limit = 100): SafeNodeDetail[] {
  if (!Array.isArray(value)) throw new ProjectionError();
  return value.map((entry) => {
    const node = record(entry);
    const operations = record(node.operations);
    const events = record(node.events);
    const projectedOperations = Object.entries(operations).map(([name, descriptor]) => {
      const item = record(descriptor);
      const capabilities = record(item.capabilities);
      return { name: operation(name), capabilities: {
        abort: boolean(capabilities.abort), inspect: boolean(capabilities.inspect), resume: boolean(capabilities.resume),
      } };
    }).sort((a, b) => a.name.localeCompare(b.name));
    const projectedEvents = Object.keys(events).map(operation).sort();
    return { id: id(node.id), connector_id: id(node.connector_id), operations: projectedOperations, events: projectedEvents };
  }).sort((a, b) => a.id.localeCompare(b.id)).slice(0, limit);
}

export function projectRelations(value: unknown, limit = 100): SafeRelation[] {
  if (!Array.isArray(value)) throw new ProjectionError();
  return value.map((entry) => {
    const snapshot = record(entry);
    const item = record(snapshot.value);
    return { id: id(item.id), revision: revision(snapshot.revision), removed: item.removed === undefined ? false : boolean(item.removed),
      source: ref(item.source), event: operation(item.event), target: ref(item.target), operation: operation(item.operation) };
  }).sort((a, b) => a.id.localeCompare(b.id)).slice(0, limit);
}

export function projectConversations(value: unknown, limit = 50): SafeConversation[] {
  if (!Array.isArray(value)) throw new ProjectionError();
  return value.map((entry) => {
    const snapshot = record(entry);
    const item = record(snapshot.value);
    if (!Array.isArray(item.participants)) throw new ProjectionError();
    const participants = item.participants.map((entry) => {
      const participant = record(entry);
      const projected: SafeConversation["participants"][number] = { ref: ref(participant.ref) };
      if (participant.receiver !== undefined && participant.receiver !== null) {
        const receiver = record(participant.receiver);
        projected.receiver = { target: ref(receiver.target), operation: operation(receiver.operation) };
      }
      return projected;
    });
    return { id: id(item.id), revision: revision(snapshot.revision), closed: item.closed === undefined ? false : boolean(item.closed), participants };
  }).sort((a, b) => a.id.localeCompare(b.id)).slice(0, limit);
}

export function projectLogs(value: unknown, limit = 100): { entries: SafeLogEntry[]; next_cursor: string } {
  const page = record(value);
  if (!Array.isArray(page.entries)) throw new ProjectionError();
  const nextCursor = boundedText(page.next_cursor, 1024, true);
  const entries = page.entries.slice(0, limit).map((entry) => {
    const row = record(entry);
    const envelope = record(row.event);
    const event: SafeLogEntry["event"] = {
      id: id(envelope.id), source: ref(envelope.source), name: operation(envelope.name),
      category: boundedText(envelope.category, 32), occurred_at: timestamp(envelope.occurred_at),
    };
    for (const key of ["call_id", "correlation_id", "causation_id", "conversation_id"] as const) {
      const value = optionalId(envelope, key);
      if (value !== undefined) event[key] = value;
    }
    const projected: SafeLogEntry = { sequence: sequence(row.sequence), event };
    if (envelope.category === "observation" && envelope.name === "state.changed" && event.source.namespace === "calls") {
      if (event.source.id !== event.call_id) projected.projection_error = "invalid_state_observation";
      else try {
        const payload = record(envelope.payload);
        const before = revision(payload.before_revision);
        const after = revision(payload.after_revision);
        if (after !== before + 1) throw new ProjectionError();
        projected.transition_observation = { state: state(payload.state), before_revision: before, after_revision: after };
      } catch { projected.projection_error = "invalid_state_observation"; }
    }
    return projected;
  });
  return { entries, next_cursor: nextCursor };
}

export function mergeJournalEvidence(previous: SafeLogEntry[], incoming: SafeLogEntry[], previousConflictIds: string[] = []): JournalEvidence {
  const byId = new Map(previous.map((entry) => [entry.event.id, entry]));
  const conflictIds = new Set(previousConflictIds);
  let added = 0;
  for (const entry of incoming) {
    const existing = byId.get(entry.event.id);
    if (existing && JSON.stringify(existing) !== JSON.stringify(entry)) conflictIds.add(entry.event.id);
    else if (!existing) { byId.set(entry.event.id, entry); added++; }
  }
  const entries = [...byId.values()].slice(-200);
  const retainedIds = new Set(entries.map((entry) => entry.event.id));
  return { entries, conflictIds: [...conflictIds].filter((id) => retainedIds.has(id)), truncated: previous.length + added > 200 };
}

export function hasContinuousJournal(entries: SafeLogEntry[]): boolean {
  if (entries.some((entry) => entry.sequence === null)) return false;
  const sorted = [...entries].sort((a, b) => compareJournalSequence(a.sequence!, b.sequence!));
  for (let index = 1; index < sorted.length; index++) {
    const previous = BigInt(String(sorted[index - 1]!.sequence));
    const current = BigInt(String(sorted[index]!.sequence));
    if (current !== previous + 1n) return false;
  }
  return true;
}

export function continuousCallTransitionPairs(entries: SafeLogEntry[], callId: string, journalAvailable: boolean, conflictIds: string[] = []): Array<[SafeLogEntry, SafeLogEntry]> {
  if (!journalAvailable) return [];
  const candidates = entries.filter((entry) => entry.event.name === "state.changed" && entry.event.category === "observation" &&
    (entry.event.call_id === callId || (entry.event.source.namespace === "calls" && entry.event.source.id === callId)));
  if (candidates.some((entry) => entry.sequence === null)) return [];
  const ordered = [...entries].filter((entry) => entry.sequence !== null).sort((a, b) => compareJournalSequence(a.sequence!, b.sequence!));
  const relevant = candidates.sort((a, b) => compareJournalSequence(a.sequence!, b.sequence!));
  const conflicts = new Set(conflictIds);
  const beforeRevisions = new Set<number>(); const afterRevisions = new Set<number>();
  for (const entry of relevant) {
    const observation = entry.transition_observation;
    if (!observation) continue;
    if (beforeRevisions.has(observation.before_revision) || afterRevisions.has(observation.after_revision)) return [];
    beforeRevisions.add(observation.before_revision); afterRevisions.add(observation.after_revision);
  }
  const pairs: Array<[SafeLogEntry, SafeLogEntry]> = [];
  for (let index = 1; index < relevant.length; index++) {
    const previous = relevant[index - 1]!; const next = relevant[index]!;
    const before = previous.transition_observation; const after = next.transition_observation;
    if (previous.projection_error || next.projection_error || !before || !after || before.after_revision !== after.before_revision) continue;
    const from = BigInt(String(previous.sequence)); const to = BigInt(String(next.sequence));
    if (to <= from) continue;
    const interval = ordered.filter((entry) => BigInt(String(entry.sequence)) >= from && BigInt(String(entry.sequence)) <= to);
    let continuous = interval.length === Number(to - from + 1n);
    for (let position = 1; continuous && position < interval.length; position++) {
      if (BigInt(String(interval[position]!.sequence)) !== BigInt(String(interval[position - 1]!.sequence)) + 1n) continuous = false;
    }
    if (!continuous || interval.some((entry) => conflicts.has(entry.event.id))) continue;
    pairs.push([previous, next]);
  }
  return pairs;
}

function compareJournalSequence(left: number | string, right: number | string): number {
  const a = BigInt(String(left)); const b = BigInt(String(right));
  return a < b ? -1 : a > b ? 1 : 0;
}

export function parseListLimit(params: URLSearchParams, allowed: string[] = ["limit"], fallback = 50, maximum = 100): number {
  const values = new Map<string, string>();
  for (const [key, value] of params) {
    if (!allowed.includes(key) || values.has(key)) throw new Error("invalid_query");
    values.set(key, value);
  }
  const raw = values.get("limit");
  if (raw === undefined) return fallback;
  if (!/^[0-9]+$/u.test(raw)) throw new Error("invalid_query");
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed) || parsed < 1 || parsed > maximum) throw new Error("invalid_query");
  return parsed;
}

export function parseLogsQuery(params: URLSearchParams): { call_id?: string; correlation_id?: string; conversation_id?: string; after?: string; limit: number } {
  const allowed = new Set(["call_id", "correlation_id", "conversation_id", "after", "limit"]);
  const values = new Map<string, string>();
  for (const [key, value] of params) {
    if (!allowed.has(key) || values.has(key)) throw new Error("invalid_query");
    values.set(key, value);
  }
  const query: { call_id?: string; correlation_id?: string; conversation_id?: string; after?: string; limit: number } = { limit: 100 };
  for (const key of ["call_id", "correlation_id", "conversation_id"] as const) {
    const value = values.get(key);
    if (value !== undefined) query[key] = id(value);
  }
  const after = values.get("after");
  if (after !== undefined) {
    if (new TextEncoder().encode(after).byteLength > 1024 || /[\u0000-\u001f\u007f]/u.test(after)) throw new Error("invalid_query");
    query.after = after;
  }
  const limit = values.get("limit");
  if (limit !== undefined) {
    if (!/^[0-9]+$/u.test(limit)) throw new Error("invalid_query");
    const parsed = Number(limit);
    if (!Number.isSafeInteger(parsed) || parsed < 1 || parsed > 200) throw new Error("invalid_query");
    query.limit = parsed;
  }
  return query;
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
  requireSameOriginFetchSite?: boolean;
}): boolean {
  const loopbackPeers = new Set(["127.0.0.1", "::1", "::ffff:127.0.0.1"]);
  if (!input.remoteAddress || !loopbackPeers.has(input.remoteAddress)) return false;
  if (!input.host) return false;
  const expectedHosts = new Set([`127.0.0.1:${input.port}`, `localhost:${input.port}`]);
  if (!expectedHosts.has(input.host.toLowerCase())) return false;
  if (input.requireSameOriginFetchSite && input.fetchSite !== undefined && input.fetchSite !== "same-origin") return false;
  if (input.fetchSite && !["same-origin", "none"].includes(input.fetchSite)) return false;
  if (input.origin) return input.origin === `http://${input.host}`;
  return input.fetchSite === "same-origin" || input.fetchSite === "none";
}

export function mutationResponseIsCurrent(actionGeneration: number, currentGeneration: number, actionInstanceId: string, currentInstanceId: string): boolean {
  return actionGeneration === currentGeneration && actionInstanceId === currentInstanceId;
}

export function subscriptionCanvasExtent(viewportWidth: number, viewportHeight: number, cardBounds: Array<{ right: number; bottom: number }>, paddingRight: number, paddingBottom: number): { width: number; height: number } {
  return {
    width: Math.max(viewportWidth, ...cardBounds.map((card) => card.right + paddingRight)),
    height: Math.max(viewportHeight, ...cardBounds.map((card) => card.bottom + paddingBottom)),
  };
}
