import React, { useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import { confirmedCoreDisplayState, coreInstanceChanged, keepHighestCallRevisions, type CallsResponse, type OverviewResponse, type SafeCall } from "./projection.js";
import "./styles.css";

type Page = "home" | "calls";
type Resource<T> = { value: T | null; error: string | null; fetchedAt: number | null; stale: boolean };
const empty = <T,>(): Resource<T> => ({ value: null, error: null, fetchedAt: null, stale: false });

function usePoll<T>(url: string, enabled: boolean, interval = 2_000, resetKey = "", mergeRevisions = true): Resource<T> {
  const [resource, setResource] = useState<Resource<T>>(empty);
  const generation = useRef(0);
  const dataUrl = useRef<string | null>(null);
  const instanceKey = useRef<string | null>(null);
  useEffect(() => {
    if (!enabled) return;
    if (instanceKey.current !== null && instanceKey.current !== resetKey) {
      dataUrl.current = null;
      setResource(empty<T>());
    }
    instanceKey.current = resetKey;
    const cacheKey = `${url}\u0000${resetKey}`;
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let active: AbortController | undefined;
    let failures = 0;
    setResource((previous) => previous.value ? { ...previous, stale: true, error: null } : previous);
    const refresh = async () => {
      const mine = ++generation.current;
      const controller = new AbortController();
      active = controller;
      try {
        const response = await fetch(url, { signal: controller.signal, headers: { Accept: "application/json" } });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const value = await response.json() as T & { meta?: { stale?: boolean; errors?: Record<string, string> } };
        if (stopped || mine !== generation.current) return;
        failures = 0;
        const issue = value.meta?.errors ? Object.keys(value.meta.errors).join(", ") : null;
        setResource((previous) => {
          let latest: T = value;
          if (dataUrl.current === cacheKey) latest = keepHighestCallRevisions(previous.value, value, mergeRevisions);
          dataUrl.current = cacheKey;
          return { value: latest, error: issue, fetchedAt: Date.now(), stale: Boolean(value.meta?.stale || issue) };
        });
      } catch (error) {
        if (stopped || mine !== generation.current || (error instanceof DOMException && error.name === "AbortError")) return;
        failures += 1;
        setResource((previous) => ({ ...previous, error: "Refresh unavailable", stale: previous.value !== null }));
      }
      if (!stopped) timer = setTimeout(refresh, Math.min(interval * 2 ** failures, 10_000));
    };
    void refresh();
    return () => { stopped = true; generation.current += 1; active?.abort(); if (timer) clearTimeout(timer); };
  }, [url, enabled, interval, resetKey]);
  return resource;
}

function timeLabel(value: number | null): string {
  if (!value) return "Waiting for first snapshot";
  return `Updated ${new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" }).format(value)}`;
}

function App() {
  const [page, setPage] = useState<Page>("home");
  const [query, setQuery] = useState("");
  const [stateFilter, setStateFilter] = useState("");
  const [selected, setSelected] = useState<string | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [visible, setVisible] = useState(!document.hidden);
  useEffect(() => {
    const update = () => setVisible(!document.hidden);
    document.addEventListener("visibilitychange", update);
    return () => document.removeEventListener("visibilitychange", update);
  }, []);
  const overview = usePoll<OverviewResponse>("/api/overview", visible);
  const callsUrl = useMemo(() => {
    const params = new URLSearchParams();
    if (query.trim()) params.set("correlation_id", query.trim());
    if (stateFilter) params.set("state", stateFilter);
    params.set("limit", "50");
    return `/api/calls?${params.toString()}`;
  }, [query, stateFilter]);
  const coreDisplay = confirmedCoreDisplayState(overview.value, overview.stale, overview.error);
  const confirmedInstanceId = coreDisplay === "unconfirmed" || coreDisplay === "connecting" ? "" : overview.value?.core?.instance_id ?? "";
  const calls = usePoll<CallsResponse>(callsUrl, visible && page === "calls", 2_000, confirmedInstanceId, Boolean(confirmedInstanceId));
  const previousInstance = useRef<string | null>(null);
  const [instanceChanged, setInstanceChanged] = useState(false);
  useEffect(() => {
    if (!confirmedInstanceId) return;
    if (coreInstanceChanged(previousInstance.current, confirmedInstanceId)) setInstanceChanged(true);
    previousInstance.current = confirmedInstanceId;
  }, [confirmedInstanceId]);
  const selectedCall = calls.value?.calls?.find((item) => item.call.id === selected) ?? null;
  const selectedNode = overview.value?.nodes?.find((node) => node.id === selectedNodeId) ?? null;

  return <div className="app-shell">
    <aside className="rail" aria-label="Primary navigation">
      <div className="brand-mark" aria-hidden="true">R</div>
      <button className={page === "home" ? "rail-link selected" : "rail-link"} onClick={() => setPage("home")} aria-current={page === "home" ? "page" : undefined} aria-label="Home · 홈"><span aria-hidden="true">⌂</span><small>Home · 홈</small></button>
      <button className={page === "calls" ? "rail-link selected" : "rail-link"} onClick={() => setPage("calls")} aria-current={page === "calls" ? "page" : undefined} aria-label="Calls · 호출"><span aria-hidden="true">◷</span><small>Calls · 호출</small></button>
      <div className="rail-bottom"><span className="rail-dot" aria-hidden="true"/><small>LOCAL</small></div>
    </aside>
    <main className="main-area">
      <header className="topbar">
        <div><div className="eyebrow">ROSTER / LOCAL CONTROL · 로컬 제어</div><h1>{page === "home" ? "Your workspace · 내 작업실" : "Call ledger · 호출 기록"}</h1></div>
        <div className="sync-label"><span className={coreDisplay === "ready" ? "status-dot ready" : coreDisplay === "unconfirmed" ? "status-dot stale" : "status-dot"} aria-hidden="true"/>{coreDisplay === "ready" ? "Core ready · Core 준비됨" : coreDisplay === "stopped" ? "Core stopped · Core 중지됨" : coreDisplay === "not_ready" ? "Core not ready · Core 준비 안 됨" : coreDisplay === "unconfirmed" ? "Core status unconfirmed · Core 상태 미확인" : "Connecting to Core · Core 연결 중"}</div>
      </header>
      {page === "home" ? <section className="home-content" aria-labelledby="room-title">
        <div className="section-heading"><div><div className="eyebrow">LIVE FROM CORE · Core 실시간 정보</div><h2 id="room-title">The room · 작업실</h2></div><span className="quiet">{timeLabel(overview.fetchedAt)}</span></div>
        <p className="model-note">Nodes are registered capabilities; Calls are individual executions. · 노드는 등록된 기능 단위이고, 호출은 각각의 실행 건입니다.</p>
        {(overview.error || overview.stale) && <div className="notice" role="status">{overview.stale ? "Stale snapshot · 오래된 스냅샷" : "Refresh issue · 갱신 문제"}{overview.error ? ` · ${overview.error}` : ""}. Last available values are retained where possible. · 가능한 경우 마지막 확인 값을 유지합니다.</div>}
        {instanceChanged && <div className="notice instance-notice" role="status">Core instance changed. In-memory records may have restarted; a fresh query cycle has begun. · Core 인스턴스가 변경되었습니다. 메모리 기록이 재시작되었을 수 있으며 새 조회를 시작했습니다.</div>}
        <div className="room-scene" aria-label="Workspace map of registered Nodes" role="group">
          <div className="room-back wall"><span className="wall-label">ROSTER WORKROOM</span><span className="wall-stamp">01 / LOCAL</span></div>
          <div className="room-floor"/>
          <div className={`room-center core-${coreDisplay}`}><div className="core-orb" aria-hidden="true"><span>CORE</span></div><strong>{coreDisplay === "ready" ? "Core is ready · Core 준비됨" : coreDisplay === "stopped" ? "Core is stopped · Core 중지됨" : coreDisplay === "not_ready" ? "Core is not ready · Core 준비 안 됨" : coreDisplay === "connecting" ? "Connecting to Core · Core 연결 중" : "Core status unconfirmed · Core 상태 미확인"}</strong><span>{overview.value?.core?.pending_delivery_count ?? "—"} pending deliveries · 대기 중 전달 · {overview.value?.core?.unresolved_call_ids.length ?? "—"} unresolved · 미확인</span></div>
          {overview.value?.nodes?.slice(0, 4).map((node, index) => <button type="button" className={`node-desk desk-${index}${selectedNodeId === node.id ? " node-selected" : ""}`} key={node.id} title={`Registered Node · 등록된 노드 ${node.id}`} aria-label={`Select Node ${node.id} · 노드 선택`} aria-pressed={selectedNodeId === node.id} onClick={() => setSelectedNodeId(node.id)}><span className="desk-lamp" aria-hidden="true"/><strong>{node.id}</strong><small>{node.connector_id}</small><em>REGISTERED · 등록됨</em></button>)}
          {(overview.value?.nodes?.length ?? 0) > 4 && <div className="more-nodes">+{(overview.value?.nodes?.length ?? 0) - 4} more registered · 추가 등록 노드</div>}
          {overview.value?.nodes?.length === 0 && <div className="room-empty"><span className="empty-icon" aria-hidden="true">＋</span><strong>No Nodes registered · 등록된 노드 없음</strong><span>Core’s current catalog · Core 현재 카탈로그</span></div>}
          {overview.value?.nodes === null && <div className="room-empty"><strong>Node catalog unavailable · 노드 목록 확인 불가</strong><span>Registration status unconfirmed · 등록 상태 미확인</span></div>}
          <div className="floor-line floor-one"/><div className="floor-line floor-two"/>
        </div>
        <div className="source-strip" aria-label="Overview query timestamps · 개요 조회 시각">
          {([ ["core", "Core"], ["nodes", "Nodes · 노드"], ["calls", "Calls · 호출"] ] as const).map(([key, label]) => {
            const source = overview.value?.sources[key];
            return <span key={key}><strong>{label}</strong><time>{source?.attempted_at ? new Date(source.attempted_at).toLocaleTimeString() : "—"}</time><em className={source?.error ? "source-error" : "source-ok"}>{source?.error ? "error · 오류" : "queried · 조회"}</em></span>;
          })}
        </div>
        {(overview.value?.sources.nodes.truncated || overview.value?.sources.calls.truncated) && <p className="quiet truncation">{overview.value.sources.nodes.truncated ? "First 100 registered Nodes shown · 등록된 노드 상위 100개 표시" : ""}{overview.value.sources.nodes.truncated && overview.value.sources.calls.truncated ? " · " : ""}{overview.value.sources.calls.truncated ? "First 12 Calls shown in this room preview · 작업실 호출 미리보기 상위 12개 표시" : ""}</p>}
        <div className="node-directory" role="group" aria-label="Registered Nodes · 등록된 노드">
          <div className="node-directory-head"><strong>Registered Nodes · 등록된 노드</strong><span>{overview.value?.nodes?.length ?? "—"}</span></div>
          {overview.value?.nodes?.map((node) => <button type="button" className={selectedNodeId === node.id ? "node-directory-item chosen" : "node-directory-item"} key={node.id} aria-pressed={selectedNodeId === node.id} onClick={() => setSelectedNodeId(node.id)}><span>{node.id}</span><small>{node.connector_id}</small><em>REGISTERED · 등록됨</em></button>)}
          {overview.value?.nodes?.length === 0 && <p>No registered Nodes · 등록된 노드가 없습니다.</p>}
          {overview.value?.nodes === null && <p>Node catalog unavailable · 노드 목록 확인 불가</p>}
          {selectedNode && <p className="node-selection" role="status">Selected Node · 선택된 노드: {selectedNode.id} · Connector · 커넥터: {selectedNode.connector_id}</p>}
        </div>
        <div className="home-foot"><div className="metric"><span>CALLS SHOWN · 표시된 호출</span><strong>{overview.value?.calls?.length ?? "—"}</strong><small>Current Core preview · Core 호출 미리보기</small></div><div className="metric"><span>DELIVERY QUEUE · 전달 대기열</span><strong>{overview.value?.core?.pending_delivery_count ?? "—"}</strong><small>Pending delivery intents · 전달 대기 작업</small></div><div className="home-note"><span className="note-mark" aria-hidden="true">↗</span><span>Every marker is from Core’s Node catalog. · 모든 표시는 Core 노드 목록에서 가져옵니다.</span><button onClick={() => setPage("calls")}>View calls · 호출 보기 <span aria-hidden="true">→</span></button></div></div>
      </section> : <section className="calls-content" aria-labelledby="calls-title">
        <div className="section-heading"><div><div className="eyebrow">READ-ONLY SNAPSHOT · 읽기 전용 스냅샷</div><h2 id="calls-title">Calls · 호출</h2></div><span className="quiet">{timeLabel(calls.fetchedAt)}</span></div>
        <p className="model-note">State labels show the latest Core lifecycle snapshot; this view does not invent a transition history. · 상태 표시는 Core 최신 수명주기 스냅샷이며, 이 화면은 전이 이력을 만들어내지 않습니다.</p>
        {instanceChanged && <div className="notice instance-notice" role="status">Core instance changed. In-memory records may have restarted; a fresh query cycle has begun. · Core 인스턴스가 변경되었습니다. 메모리 기록이 재시작되었을 수 있으며 새 조회를 시작했습니다.</div>}
        <div className="call-toolbar"><label className="search-field"><span className="sr-only">Filter by correlation ID · 상관 ID 필터</span><span aria-hidden="true">⌕</span><input value={query} onChange={(event) => { setSelected(null); setQuery(event.target.value); }} placeholder="Correlation ID · 상관 ID" maxLength={100}/></label><label className="filter-field"><span className="sr-only">Filter by state · 상태 필터</span><select value={stateFilter} onChange={(event) => { setSelected(null); setStateFilter(event.target.value); }}><option value="">All states · 모든 상태</option>{["created","queued","preparing","dispatching","accepted","running","waiting","resuming","cancelling","unknown","reconciling","succeeded","failed","cancelled","expired","rejected"].map((state) => <option key={state} value={state}>{stateLabel(state)}</option>)}</select></label><span className="count-label">{calls.value?.calls !== null && calls.value ? `${calls.value.meta.returned_count} shown · 표시` : "— shown · 표시"}</span></div>
        {(calls.error || calls.stale) && <div className="notice" role="status">{calls.stale ? "Stale snapshot · 오래된 스냅샷" : "Refresh issue · 갱신 문제"}{calls.error ? ` · ${calls.error}` : ""}. The previous valid list is retained where possible. · 가능한 경우 마지막 유효 목록을 유지합니다.</div>}
        <div className="ledger-layout"><div className="table-wrap"><table><thead><tr><th scope="col">CALL · 호출</th><th scope="col">TARGET · 대상</th><th scope="col">OPERATION · 작업</th><th scope="col">STATE · 상태</th><th scope="col">UPDATED · 갱신</th></tr></thead><tbody>
          {calls.value?.calls?.map((call) => <tr key={call.call.id} className={selected === call.call.id ? "active-row" : ""}><td><button className="call-select" onClick={() => setSelected(call.call.id)} aria-pressed={selected === call.call.id}>{call.call.id}</button></td><td>{call.call.target.id}</td><td><code>{call.call.operation}</code></td><td><StateBadge call={call}/></td><td>{new Date(call.lifecycle.updated_at).toLocaleTimeString()}</td></tr>)}
          {calls.value?.calls?.length === 0 && <tr><td colSpan={5} className="empty-row">No calls match this query. · 조건에 맞는 호출이 없습니다.</td></tr>}
          {calls.value?.calls === null && <tr><td colSpan={5} className="empty-row">No verified call snapshot is available. · 확인된 호출 스냅샷이 없습니다.</td></tr>}
          {!calls.value && <tr><td colSpan={5} className="empty-row">{calls.error ? "Call snapshot unavailable · 호출 스냅샷 확인 불가" : "Loading Core call snapshot… · Core 호출 스냅샷 불러오는 중"}</td></tr>}
        </tbody></table></div>
        <aside className="detail-panel" aria-label="Selected call details · 선택한 호출 세부 정보">{selectedCall ? <CallDetail call={selectedCall}/> : <div className="detail-empty"><span aria-hidden="true">◉</span><strong>Select a call · 호출 선택</strong><p>Safe call details will appear here. · 안전한 호출 세부 정보가 여기에 표시됩니다.</p></div>}</aside></div>
        {calls.value?.meta.truncated && <p className="quiet truncation">Showing the first {calls.value.meta.returned_count} calls. Narrow the query to inspect a smaller set.</p>}
      </section>}
      <footer className="page-footer"><span>ROSTER DASHBOARD <b>·</b> LOCAL READ-ONLY VIEW</span><span>{overview.value?.core?.instance_id ?? "Core instance unconfirmed"}</span></footer>
    </main>
  </div>;
}

function StateBadge({ call }: { call: SafeCall }) {
  return <span className={`state-badge state-${call.lifecycle.state}`}><i aria-hidden="true"/>{stateLabel(call.lifecycle.state)}</span>;
}

function stateLabel(state: string): string {
  const labels: Record<string, string> = {
    created: "created · 생성됨", validating: "validating · 검증 중", rejected: "rejected · 거부됨",
    queued: "queued · 대기열", preparing: "preparing · 준비 중", dispatching: "dispatching · 전달 중",
    accepted: "accepted · 수락됨", running: "running · 실행 중", waiting: "waiting · 대기 중",
    resuming: "resuming · 재개 중", cancelling: "cancelling · 취소 중", unknown: "unknown · 알 수 없음",
    reconciling: "reconciling · 조정 중", succeeded: "succeeded · 성공", failed: "failed · 실패",
    cancelled: "cancelled · 취소됨", expired: "expired · 만료됨",
  };
  return labels[state] ?? state;
}

function CallDetail({ call }: { call: SafeCall }) {
  return <div className="call-detail"><div className="eyebrow">CALL DETAIL</div><h3>{call.call.operation}</h3><StateBadge call={call}/><dl><dt>Call ID</dt><dd>{call.call.id}</dd><dt>Target</dt><dd>{call.call.target.namespace} / {call.call.target.id}</dd><dt>Correlation</dt><dd>{call.call.correlation_id ?? "—"}</dd><dt>Updated</dt><dd>{new Date(call.lifecycle.updated_at).toLocaleString()}</dd><dt>Result</dt><dd>{call.result ? `${call.result.state}${call.result.error_present ? " · error present" : ""}` : "Not reported"}</dd><dt>Abort requested</dt><dd>{call.abort_requested ? "Yes" : "No"}</dd></dl><p className="detail-caption">Payloads and result contents are intentionally hidden.</p></div>;
}

createRoot(document.getElementById("root")!).render(<React.StrictMode><App/></React.StrictMode>);
