# Roster Dashboard

로컬 Roster Core를 읽기 전용으로 보여주는 대시보드입니다. Dashboard는 `127.0.0.1:7360`, Core는 `127.0.0.1:7350`에서 실행해야 합니다.

A read-only dashboard for a local Roster Core instance. The Dashboard uses `127.0.0.1:7360`; Core must be available at `127.0.0.1:7350`.

## 실행 · Run

Node.js 24 이상이 필요합니다. 저장소 루트에서 다음 명령을 실행한 뒤 `http://127.0.0.1:7360`을 여세요.

Requires Node.js 24 or newer. Run these commands from the repository root, then open `http://127.0.0.1:7360`.

```sh
npm ci
npm test
npm run build
npm start
```

서버는 loopback에만 바인딩합니다. 다른 로컬 포트가 필요하면 `PORT`를 설정하세요. 브라우저 API는 동일 origin의 `GET /api/overview`와 `GET /api/calls`만 제공합니다.

The server binds only to loopback. Set `PORT` to choose another local port. The browser API exposes only same-origin `GET /api/overview` and `GET /api/calls`.

## 범위 · Scope

홈 화면은 Core의 준비 상태, 등록된 Node, 현재 Call을 구분해 보여줍니다. Node 등록은 실행 가능 상태나 온라인 상태를 뜻하지 않습니다. Call 상태는 Core가 관측한 최신 스냅샷이며 업무 성공을 추정하지 않습니다.

Home separates Core readiness, registered Nodes, and current Calls. Registration does not indicate that a Node is available or online. Call state is Core's latest observed snapshot and does not infer business success.

부분 실패와 오래된 데이터는 표시됩니다. Core 인스턴스가 바뀌면 메모리 기록이 재시작되었을 수 있음을 알립니다. Call 입력, Result 출력, Node binding, Event payload 및 대화 본문은 브라우저에 전달하지 않습니다.

Partial failures and stale data remain visible. A changed Core instance is reported because its in-memory records may have restarted. Call inputs, Result outputs, Node bindings, Event payloads, and conversation bodies are not sent to the browser.
