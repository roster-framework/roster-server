import React, { useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import { confirmedCoreDisplayState, continuousCallTransitionPairs, coreInstanceChanged, hasContinuousJournal, keepHighestCallRevisions, mergeJournalEvidence, type CallsResponse, type LogsResponse, type OverviewResponse, type SafeCall, type SafeConversation, type SafeLogEntry, type SafeNodeDetail, type SafeRelation } from "./projection.js";
import "./styles.css";

type Page = "home" | "calls" | "relations";
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

type RelationSelection = { kind: "node" | "call" | "conversation" | "event" | "subscription"; id: string } | null;

function RelationsView({ visible, instanceId, instanceChanged, openCalls }: { visible: boolean; instanceId: string; instanceChanged: boolean; openCalls: () => void }) {
  const [selected, setSelected] = useState<RelationSelection>(null);
  const [afterCursor, setAfterCursor] = useState("");
  const [journalEntries, setJournalEntries] = useState<SafeLogEntry[]>([]);
  const journalEntriesRef = useRef<SafeLogEntry[]>([]);
  const [journalConflictIds, setJournalConflictIds] = useState<string[]>([]);
  const journalConflictIdsRef = useRef<string[]>([]);
  const [journalTruncated, setJournalTruncated] = useState(false);
  const priorInstance = useRef(instanceId);
  const nodes = usePoll<{ meta: { returned_count: number; truncated: boolean; errors: Record<string, string> }; nodes: SafeNodeDetail[] | null }>("/api/nodes", visible, 2_000, instanceId);
  const relations = usePoll<{ meta: { returned_count: number; truncated: boolean; errors: Record<string, string> }; relations: SafeRelation[] | null }>("/api/relations", visible, 2_000, instanceId);
  const conversations = usePoll<{ meta: { returned_count: number; truncated: boolean; errors: Record<string, string> }; conversations: SafeConversation[] | null }>("/api/conversations?limit=100", visible, 2_000, instanceId);
  const calls = usePoll<CallsResponse>("/api/calls?limit=100", visible, 2_000, instanceId, Boolean(instanceId));
  const logsUrl = "/api/logs?limit=200" + (afterCursor ? "&after=" + encodeURIComponent(afterCursor) : "");
  const logs = usePoll<LogsResponse>(logsUrl, visible, 2_000, instanceId);
  const nodeItems = nodes.value?.nodes ?? [];
  const relationItems = relations.value?.relations ?? [];
  const callItems = calls.value?.calls ?? [];
  const conversationItems = conversations.value?.conversations ?? [];
  const logItems = journalEntries;
  const nodesConfirmed = Boolean(nodes.value?.nodes && !nodes.error && !nodes.stale);
  const relationsConfirmed = Boolean(relations.value?.relations && !relations.error && !relations.stale);
  const callsConfirmed = Boolean(calls.value?.calls && !calls.error && !calls.stale);
  const conversationsConfirmed = Boolean(conversations.value?.conversations && !conversations.error && !conversations.stale);
  useEffect(() => {
    if (priorInstance.current !== instanceId) {
      setAfterCursor("");
      setJournalEntries([]);
      journalEntriesRef.current = [];
      journalConflictIdsRef.current = [];
      setJournalConflictIds([]);
      setJournalTruncated(false);
      setSelected(null);
      priorInstance.current = instanceId;
    }
  }, [instanceId]);
  useEffect(() => {
    if (!logs.value?.entries) return;
    const merged = mergeJournalEvidence(journalEntriesRef.current, logs.value.entries, journalConflictIdsRef.current);
    journalEntriesRef.current = merged.entries;
    setJournalEntries(merged.entries);
    journalConflictIdsRef.current = merged.conflictIds;
    setJournalConflictIds(merged.conflictIds);
    if (merged.truncated) setJournalTruncated(true);
  }, [logs.value]);
  const sceneNodes = nodeItems.slice(0, 8);
  const sceneCalls = callItems.slice(0, 8);
  const selectedCall = selected?.kind === "call" ? callItems.find((item) => item.call.id === selected.id) : undefined;
  const selectedNode = selected?.kind === "node" ? nodeItems.find((item) => item.id === selected.id) : undefined;
  const selectedConversation = selected?.kind === "conversation" ? conversationItems.find((item) => item.id === selected.id) : undefined;
  const nodeX = (index: number) => 100 + index * (800 / Math.max(sceneNodes.length - 1, 1));
  const callX = (index: number) => 80 + index * (840 / Math.max(sceneCalls.length - 1, 1));
  const findNode = (namespace: string, id: string) => namespace === "nodes" ? sceneNodes.findIndex((item) => item.id === id) : -1;
  const findCall = (id: string) => callItems.findIndex((item) => item.call.id === id);
  const findSceneCall = (id: string) => sceneCalls.findIndex((item) => item.call.id === id);
  const logById = new Map(logItems.map((item) => [item.event.id, item]));
  const causeEdges = sceneCalls.flatMap((call) => {
    const cause = call.call.causation_id;
    if (!cause) return [];
    const sourceIndex = findSceneCall(cause);
    return sourceIndex >= 0 ? [{ call, sourceIndex, cause }] : [];
  });
  const causeEventEdges = sceneCalls.flatMap((call) => {
    const cause = call.call.causation_id;
    const event = cause ? logById.get(cause) : undefined;
    return event ? [{ call, event }] : [];
  });
  const causeEvents = [...new Map(causeEventEdges.map((item) => [item.event.event.id, item.event])).values()].slice(0, 5);
  const groups = new Map<string, SafeCall[]>();
  for (const call of sceneCalls) if (call.call.correlation_id) groups.set(call.call.correlation_id, [...(groups.get(call.call.correlation_id) ?? []), call]);
  const callEvents = selectedCall ? logItems.filter((item) => item.event.call_id === selectedCall.call.id ||
    (item.event.name === "state.changed" && item.event.category === "observation" && item.event.source.namespace === "calls" && item.event.source.id === selectedCall.call.id)).sort((a, b) => compareSequence(a.sequence, b.sequence)) : [];
  const transitions = callEvents.filter((item) => item.transition_observation);
  const hasTransitionCandidates = callEvents.some((item) => item.event.name === "state.changed" && item.event.category === "observation");
  const journalComplete = Boolean(logs.value?.entries && logs.value.entries.length < 200 && !logs.value.meta.truncated && !logs.value.cursor_expired && !logs.error && !logs.stale && journalConflictIds.length === 0 && !journalTruncated && hasContinuousJournal(logItems));
  const journalAvailable = Boolean(logs.value?.entries !== null && logs.value && !logs.value.cursor_expired && !logs.error && !logs.stale);
  const transitionPairs = selectedCall ? continuousCallTransitionPairs(logItems, selectedCall.call.id, journalAvailable, journalConflictIds) : [];
  const missingCause = callsConfirmed && journalComplete ? callItems.filter((call) => call.call.causation_id && findCall(call.call.causation_id) < 0 && !logById.has(call.call.causation_id)) : [];
  const missingTargets = callsConfirmed && nodesConfirmed ? callItems.filter((call) => call.call.target.namespace === "nodes" && !nodeItems.some((node) => node.id === call.call.target.id)) : [];
  const hiddenCallCauses = sceneCalls.filter((call) => call.call.causation_id && findCall(call.call.causation_id) >= 0 && findSceneCall(call.call.causation_id) < 0);
  const hiddenEventCauses = sceneCalls.filter((call) => call.call.causation_id && logById.has(call.call.causation_id) && !causeEvents.some((entry) => entry.event.id === call.call.causation_id));
  const hiddenSubscriptions = relationItems.filter((item) => !item.removed &&
    ((item.source.namespace === "nodes" && nodeItems.some((node) => node.id === item.source.id) && findNode(item.source.namespace,item.source.id)<0) ||
     (item.target.namespace === "nodes" && nodeItems.some((node) => node.id === item.target.id) && findNode(item.target.namespace,item.target.id)<0)));
  const unresolvedSubscriptions = nodesConfirmed && relationsConfirmed ? relationItems.filter((item) => !item.removed &&
    !(item.source.namespace === "nodes" && nodeItems.some((node) => node.id === item.source.id) &&
      item.target.namespace === "nodes" && nodeItems.some((node) => node.id === item.target.id))) : [];
  const allReferenceSourcesConfirmed = callsConfirmed && nodesConfirmed && relationsConfirmed && journalComplete;
  const focusedConversation = selected?.kind === "conversation" ? conversationItems.find((item) => item.id === selected.id) : undefined;
  const conversationNodeIds = new Set((focusedConversation?.participants ?? []).flatMap((participant) => [
    ...(participant.ref.namespace === "nodes" ? [participant.ref.id] : []),
    ...(participant.receiver?.target.namespace === "nodes" ? [participant.receiver.target.id] : []),
  ]));

  return <section className="relations-content" aria-labelledby="relations-title">
    <div className="section-heading"><div><div className="eyebrow">READ-ONLY RELATIONS · 읽기 전용 관계</div><h2 id="relations-title">Flow observatory · 흐름 관측실</h2></div><span className="quiet">{timeLabel(calls.fetchedAt)}</span></div>
    <p className="model-note">Dashed lines show declared subscriptions; solid lines require explicit retained causation. Correlation does not imply order or dependency. · 점선은 구독 선언, 실선은 보유된 명시적 원인입니다. 상관은 순서나 종속을 뜻하지 않습니다.</p>
    {instanceChanged && <div className="notice instance-notice" role="status">Core instance changed; in-memory records may have restarted. · Core 인스턴스가 변경되어 메모리 기록이 재시작되었을 수 있습니다.</div>}
    {[nodes, relations, calls, conversations, logs].some((item) => item.error || item.stale) && <div className="notice" role="status">Some evidence is stale or unavailable. · 일부 근거가 오래되었거나 조회되지 않았습니다. Sources remain separate snapshots. · 자료원은 별도 스냅샷입니다.</div>}
    {logs.value?.cursor_expired && <div className="notice" role="status">Journal cursor expired; continuity is unavailable. · 저널 커서가 만료되어 기록 연속성을 확인할 수 없습니다. <button className="inline-action" onClick={()=>{setAfterCursor("");journalEntriesRef.current=[];setJournalEntries([]);journalConflictIdsRef.current=[];setJournalConflictIds([]);setJournalTruncated(false);}}>Reload retained journal · 보유 저널 다시 불러오기</button></div>}
    <div className="relation-legend"><span><i className="legend-dash"/>Subscription declaration · 구독 선언</span><span><i className="legend-solid"/>Explicit causation · 명시적 원인</span><span><i className="legend-group"/>Correlation group · 상관 그룹</span></div>
    <div className="relation-grid">
      <div className="relation-graph-wrap"><svg className="relation-graph" viewBox="0 0 1000 430" role="img" aria-label="Registered Nodes, declarations, observed Calls · 등록 노드, 구독 선언, 관측 호출">
        <text x="24" y="28" className="svg-lane">REGISTERED NODES · 등록 노드</text><text x="24" y="233" className="svg-lane">OBSERVED CALLS · 관측 호출</text>
        {[...groups.entries()].filter((entry) => entry[1].length > 1).map(([id, items]) => { const indices = items.map((item) => findSceneCall(item.call.id)); const left = Math.min(...indices.map(callX)) - 42; const right = Math.max(...indices.map(callX)) + 42; return <g key={id}><rect x={left} y="255" width={Math.max(84, right-left)} height="145" rx="14" className="correlation-zone"/><text x={left+9} y="272" className="svg-group-label">CORRELATION · 상관 {clip(id,14)}</text></g>; })}
        {relationItems.filter((item) => !item.removed).map((item) => { const a = findNode(item.source.namespace, item.source.id); const b = findNode(item.target.namespace, item.target.id); return a >= 0 && b >= 0 ? <g key={item.id}><path d={"M " + nodeX(a) + " 103 C " + nodeX(a) + " 165, " + nodeX(b) + " 165, " + nodeX(b) + " 103"} className="subscription-edge"/><text x={(nodeX(a)+nodeX(b))/2} y="148" className="svg-edge-label" textAnchor="middle">{clip(item.event+" → "+item.operation,22)}</text><title>{item.source.id + "." + item.event + " → " + item.target.id + "." + item.operation}</title></g> : null; })}
        {causeEdges.map(({ call, sourceIndex, cause }) => { const targetIndex=findSceneCall(call.call.id); return <g key={call.call.id + cause}><path d={"M " + callX(sourceIndex) + " 345 Q " + ((callX(sourceIndex)+callX(targetIndex))/2) + " 286 " + callX(targetIndex) + " 345"} className="causation-edge"/><title>{"Explicit causation · 명시적 원인 " + cause}</title></g>; })}
        {causeEventEdges.filter(({event})=>causeEvents.some((entry)=>entry.event.id===event.event.id)).map(({call,event}) => { const index=causeEvents.findIndex((entry)=>entry.event.id===event.event.id); const x=145+(index%5)*175; const targetIndex=findSceneCall(call.call.id); return <g key={call.call.id+event.event.id}><path d={"M "+x+" 237 Q "+((x+callX(targetIndex))/2)+" 273 "+callX(targetIndex)+" 312"} className="causation-edge"/><title>{"Event reference · 이벤트 원인 "+event.event.id}</title></g>; })}
        {causeEvents.map((entry,index) => { const x=145+(index%5)*175; return <g key={entry.event.id} className="graph-node" role="button" tabIndex={0} aria-label={"Select retained Event "+entry.event.id+" · 보유 이벤트 선택"} onClick={()=>setSelected({kind:"event",id:entry.event.id})} onKeyDown={(event)=>{if(event.key==="Enter"||event.key===" "){event.preventDefault();setSelected({kind:"event",id:entry.event.id});}}}><rect x={x-55} y="192" width="110" height="45" rx="7" className={selected?.kind==="event"&&selected.id===entry.event.id?"graph-event selected":"graph-event"}/><text x={x} y="210" className="svg-title" textAnchor="middle">{clip(entry.event.name,14)}</text><text x={x} y="226" className="svg-subtitle" textAnchor="middle">{clip(entry.event.id,12)}</text></g>; })}
        {sceneNodes.map((node, index) => <g key={node.id} className="graph-node" role="button" tabIndex={0} aria-label={"Select Node " + node.id + " · 노드 선택"} onClick={() => setSelected({kind:"node",id:node.id})} onKeyDown={(event) => { if (event.key === "Enter" || event.key === " ") { event.preventDefault(); setSelected({kind:"node",id:node.id}); } }}><rect x={nodeX(index)-58} y="47" width="116" height="66" rx="9" className={selected?.kind === "node" && selected.id === node.id ? "graph-card selected" : conversationNodeIds.has(node.id) ? "graph-card conversation-highlight" : "graph-card"}/><text x={nodeX(index)} y="73" className="svg-title" textAnchor="middle">{clip(node.id,17)}</text><text x={nodeX(index)} y="94" className="svg-subtitle" textAnchor="middle">{node.operations.length} operations · 기능</text></g>)}
        {sceneCalls.map((call, index) => <g key={call.call.id} className="graph-node" role="button" tabIndex={0} aria-label={"Select Call " + call.call.id + " " + call.lifecycle.state + " · 호출 선택"} onClick={() => setSelected({kind:"call",id:call.call.id})} onKeyDown={(event) => { if (event.key === "Enter" || event.key === " ") { event.preventDefault(); setSelected({kind:"call",id:call.call.id}); } }}><rect x={callX(index)-59} y="312" width="118" height="72" rx="9" className={selected?.kind === "call" && selected.id === call.call.id ? "graph-card selected" : focusedConversation?.id === call.call.conversation_id ? "graph-card conversation-highlight" : "graph-card"}/><text x={callX(index)} y="337" className="svg-title" textAnchor="middle">{clip(call.call.id,15)}</text><text x={callX(index)} y="359" className="svg-subtitle" textAnchor="middle">{call.lifecycle.state} · r{call.revision}</text></g>)}
        {nodeItems.length === 0 && nodesConfirmed && <text x="500" y="90" className="svg-empty" textAnchor="middle">No registered Nodes · 등록 노드 없음</text>}
        {callItems.length === 0 && callsConfirmed && <text x="500" y="350" className="svg-empty" textAnchor="middle">No retained Calls · 보유 호출 없음</text>}
      </svg>
      {[nodes, calls, relations, conversations, logs].some((item) => item.value?.meta.truncated) && <p className="quiet truncation">Bounded results, not totals · 제한 조회이며 전체 합계가 아닙니다.</p>}</div>
      <aside className="relation-detail" aria-label="Selected relation details · 선택한 관계 세부 정보">
        {selectedCall ? <><div className="eyebrow">CURRENT CALL SNAPSHOT · 현재 호출 스냅샷</div><h3>{selectedCall.call.operation}</h3><StateBadge call={selectedCall}/><dl><dt>Call ID · 호출 ID</dt><dd>{selectedCall.call.id}</dd><dt>Target · 대상</dt><dd>{selectedCall.call.target.namespace}/{selectedCall.call.target.id}</dd><dt>Revision · 리비전</dt><dd>{selectedCall.revision}</dd><dt>Correlation · 상관</dt><dd>{selectedCall.call.correlation_id ?? "—"}</dd><dt>Conversation · 대화</dt><dd>{selectedCall.call.conversation_id ?? "—"}</dd></dl><h4>Retained events · 보유 이벤트</h4>{callEvents.length ? <ul className="event-list">{callEvents.map((entry) => <li key={entry.event.id}><button onClick={() => setSelected({kind:"event",id:entry.event.id})}>{entry.event.name} · {entry.event.id.slice(0,12)}</button><small>{entry.event.occurred_at}</small>{entry.transition_observation && <small>{stateLabel(entry.transition_observation.state)} · revision {entry.transition_observation.before_revision} → {entry.transition_observation.after_revision}</small>}{entry.projection_error && <small>Insufficient evidence · 근거 부족</small>}</li>)}</ul> : <p className="quiet">{journalComplete ? "No retained events · 보유 이벤트 없음" : "Event evidence unavailable · 이벤트 근거 확인 불가"}</p>}{hasTransitionCandidates && (transitionPairs.length ? transitionPairs.map(([prior,entry]) => <p className="transition-link" key={prior.event.id+"-"+entry.event.id}>{stateLabel(prior.transition_observation!.state)+" → "+stateLabel(entry.transition_observation!.state)}</p>) : <p className="transition-gap">Insufficient evidence to connect state transitions · 상태 전이 연결 근거 부족</p>)}<p className="detail-caption">Retained evidence only; current Call snapshot remains authoritative. · 보유 근거만 표시하며 현재 호출 스냅샷이 기준입니다.</p></>
        : selectedNode ? <><div className="eyebrow">REGISTERED NODE · 등록 노드</div><h3>{selectedNode.id}</h3><p>Connector · 커넥터: {selectedNode.connector_id}</p><h4>Operations · 기능</h4><ul className="operation-list">{selectedNode.operations.map((op) => <li key={op.name}><code>{op.name}</code><small>abort {op.capabilities.abort ? "yes" : "no"} · inspect {op.capabilities.inspect ? "yes" : "no"} · resume {op.capabilities.resume ? "yes" : "no"}</small></li>)}</ul><h4>Events · 이벤트</h4><p>{selectedNode.events.join(", ") || "No declared events · 선언 이벤트 없음"}</p><p className="detail-caption">Registered descriptor only; reachability is not reported. · 등록 명세만 표시하며 연결 상태는 알 수 없습니다.</p></>
        : selectedConversation ? <><div className="eyebrow">CONVERSATION · 대화</div><h3>{selectedConversation.id}</h3><p>Revision {selectedConversation.revision} · {selectedConversation.closed ? "closed · 종료" : "open · 진행"} · {selectedConversation.participants.length} participants · 참여자</p><ul className="operation-list">{selectedConversation.participants.map((p,index)=><li key={p.ref.namespace+p.ref.id+index}><code>{p.ref.namespace}/{p.ref.id}</code><small>{p.receiver ? "Receiver · 수신 설정: "+p.receiver.target.namespace+"/"+p.receiver.target.id+" · "+p.receiver.operation : "Read participant · 읽기 참여자, no receiver · 수신 설정 없음"}</small></li>)}</ul><p>Returned Calls for this Conversation · 이 대화에 연결된 반환 호출</p><ul className="event-list">{callItems.filter((item)=>item.call.conversation_id===selectedConversation.id).map((item)=><li key={item.call.id}><button onClick={()=>setSelected({kind:"call",id:item.call.id})}>{item.call.id} · {stateLabel(item.lifecycle.state)}</button></li>)}</ul><p className="detail-caption">Several Calls do not prove a single-message fan-out or collective completion. · 여러 호출은 단일 메시지 전달이나 전체 완료를 증명하지 않습니다.</p></>
        : selected?.kind === "event" ? <><div className="eyebrow">RETAINED EVENT · 보유 이벤트</div><h3>{selected.id}</h3><p>{logById.get(selected.id)?.event.name ?? "Event reference unavailable · 이벤트 참조 확인 불가"}</p><p>Sequence · 순번: {logById.get(selected.id)?.sequence ?? "—"} · {logById.get(selected.id)?.event.occurred_at}</p><p className="detail-caption">Payload omitted · 페이로드 제외</p></>
        : selected?.kind === "subscription" ? <><div className="eyebrow">DECLARED SUBSCRIPTION · 구독 선언</div><h3>{selected.id}</h3>{relationItems.filter((item)=>item.id===selected.id).map((item)=><p key={item.id}>{item.source.id}.{item.event} → {item.target.id}.{item.operation}</p>)}<p className="detail-caption">Declaration is not execution evidence · 선언은 실행 근거가 아닙니다</p></>
        : <div className="detail-empty"><span aria-hidden="true">⌁</span><strong>Select a graph item · 그래프 항목 선택</strong><p>Safe relation details appear here · 안전한 관계 세부 정보 표시</p></div>}</aside>
    </div>
    <div className="relation-lists">
      <section className="relation-list-panel"><h3>Declarations · 구독 선언</h3>{relationItems.filter((item)=>!item.removed).map((item)=><button key={item.id} className="relation-list-item" onClick={()=>setSelected({kind:"subscription",id:item.id})} aria-pressed={selected?.kind==="subscription"&&selected.id===item.id}><span>{item.source.id}.{item.event} → {item.target.id}.{item.operation}</span><small>ID {item.id} · revision {item.revision}</small></button>)}{relations.value?.relations?.length===0&&!relations.error&&!relations.stale&&<p>No active declarations · 활성 구독 선언 없음</p>}{relations.value?.relations===null&&<p>Declarations unavailable · 구독 조회 불가</p>}</section>
      <section className="relation-list-panel"><h3>Nodes · 노드</h3>{nodeItems.map((item)=><button key={item.id} className={"relation-list-item "+(focusedConversation&&conversationNodeIds.has(item.id)?"conversation-related":"")} onClick={()=>setSelected({kind:"node",id:item.id})} aria-pressed={selected?.kind==="node"&&selected.id===item.id}><span>{item.id} · {item.connector_id}</span><small>{item.operations.map((op)=>op.name).join(", ") || "No operations · 기능 없음"}</small></button>)}{nodes.value?.nodes?.length===0&&!nodes.error&&!nodes.stale&&<p>No registered Nodes · 등록 노드 없음</p>}{nodes.value?.nodes===null&&<p>Nodes unavailable · 노드 조회 불가</p>}</section>
      <section className="relation-list-panel"><h3>Calls · 호출</h3>{callItems.map((item)=><button key={item.call.id} className={"relation-list-item "+(focusedConversation&&item.call.conversation_id===focusedConversation.id?"conversation-related":"")} onClick={()=>setSelected({kind:"call",id:item.call.id})} aria-pressed={selected?.kind==="call"&&selected.id===item.call.id}><span>{item.call.id} · {item.call.target.id}.{item.call.operation} · {stateLabel(item.lifecycle.state)}</span><small>{item.call.correlation_id ? "Correlation · 상관: "+item.call.correlation_id : "No correlation ID · 상관 ID 없음"}{item.call.causation_id ? " · Cause · 원인: "+item.call.causation_id : " · No explicit cause · 명시 원인 없음"}</small></button>)}{calls.value?.calls?.length===0&&!calls.error&&!calls.stale&&<p>No retained Calls · 보유 호출 없음</p>}{calls.value?.calls===null&&<p>Calls unavailable · 호출 조회 불가</p>}</section>
      <section className="relation-list-panel"><h3>Conversations · 대화</h3>{conversationItems.map((item)=><button key={item.id} className="relation-list-item" onClick={()=>setSelected({kind:"conversation",id:item.id})} aria-pressed={selected?.kind==="conversation"&&selected.id===item.id}><span>{item.id} · {item.participants.length} participants · 참여자 · {item.closed?"closed · 종료":"open · 진행"}</span><small>{callItems.filter((call)=>call.call.conversation_id===item.id).length} returned Calls · 반환 호출 (bounded · 제한 범위)</small></button>)}{conversations.value?.conversations?.length===0&&!conversations.error&&!conversations.stale&&<p>No retained Conversations · 보유 대화 없음</p>}{conversations.value?.conversations===null&&<p>Conversations unavailable · 대화 조회 불가</p>}</section>
      <section className="relation-list-panel"><h3>Retained events · 보유 이벤트</h3>{logItems.map((item)=><button key={item.event.id} className="relation-list-item" onClick={()=>setSelected({kind:"event",id:item.event.id})} aria-pressed={selected?.kind==="event"&&selected.id===item.event.id}><span>{item.event.name} · {item.event.id}</span><small>Sequence · 순번 {item.sequence ?? "—"} · {item.event.source.namespace}/{item.event.source.id} · {item.event.occurred_at}</small></button>)}{logs.value?.entries?.length===0&&!logs.error&&!logs.stale&&!logs.value.cursor_expired&&<p>No retained Events · 보유 이벤트 없음</p>}{logs.value?.entries===null&&<p>Journal unavailable · 저널 조회 불가</p>}</section>
    </div>
    <section className="unresolved-panel"><h3>Unresolved references · 미해결 참조</h3>{missingCause.map((item)=><p key={item.call.id}>Call {item.call.id} references {item.call.causation_id}; source is not in retained evidence. · 호출 원인 참조를 보유 근거에서 찾지 못했습니다.</p>)}{missingTargets.map((item)=><p key={"target-"+item.call.id}>Call {item.call.id} targets Node {item.call.target.id}, which is not in the current catalog. · 호출 대상 노드가 현재 목록에 없습니다.</p>)}{unresolvedSubscriptions.map((item)=><p key={item.id}>Subscription {item.id} references a Node not present in the current catalog. · 구독 선언이 현재 노드 목록에 없는 노드를 참조합니다.</p>)}{allReferenceSourcesConfirmed&&missingCause.length===0&&missingTargets.length===0&&unresolvedSubscriptions.length===0&&<p>No unresolved references in this bounded view · 제한 조회에서 미해결 참조 없음</p>}{!allReferenceSourcesConfirmed&&<p>Reference evidence is incomplete; absence cannot be confirmed. · 참조 근거가 불완전하여 부재를 확정할 수 없습니다.</p>}{(hiddenCallCauses.length>0||hiddenEventCauses.length>0||hiddenSubscriptions.length>0||nodeItems.length>sceneNodes.length||callItems.length>sceneCalls.length)&&<p>Some retained items or edges are outside the SVG preview limit; HTML lists keep the full bounded result set. · 일부 보유 항목/간선은 SVG 미리보기 범위 밖이며 HTML 목록에서 전체 제한 결과를 확인할 수 있습니다.</p>}</section>
    <div className="relation-actions"><span>{journalEntries.length} deduplicated retained events · 중복 제거된 보유 이벤트</span><div>{(logs.value?.entries?.length ?? 0) >= 200 && logs.value?.next_cursor && !logs.value.cursor_expired && <button onClick={()=>setAfterCursor(logs.value!.next_cursor)}>Load next Journal page · 저널 다음 페이지 →</button>}<button onClick={openCalls}>Open Call ledger · 호출 목록 열기 →</button></div></div>
  </section>;
}

function compareSequence(left: number | string | null, right: number | string | null): number { if (left===null || right===null) return left===right ? 0 : left===null ? 1 : -1; try { const a=BigInt(String(left)), b=BigInt(String(right)); return a<b ? -1 : a>b ? 1 : 0; } catch { return String(left).localeCompare(String(right)); } }
function clip(value: string, max: number): string { return value.length>max ? value.slice(0,max-1)+"…" : value; }

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
      <button className={page === "relations" ? "rail-link selected" : "rail-link"} onClick={() => setPage("relations")} aria-current={page === "relations" ? "page" : undefined} aria-label="Relations · 관계"><span aria-hidden="true">⌁</span><small>Relations · 관계</small></button>
      <div className="rail-bottom"><span className="rail-dot" aria-hidden="true"/><small>LOCAL</small></div>
    </aside>
    <main className="main-area">
      <header className="topbar">
        <div><div className="eyebrow">ROSTER / LOCAL CONTROL · 로컬 제어</div><h1>{page === "home" ? "Your workspace · 내 작업실" : page === "calls" ? "Call ledger · 호출 기록" : "Relations · 관계"}</h1></div>
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
      </section> : page === "calls" ? <section className="calls-content" aria-labelledby="calls-title">
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
      </section> : <RelationsView visible={visible && page === "relations"} instanceId={confirmedInstanceId} instanceChanged={instanceChanged} openCalls={() => setPage("calls")}/>}
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
