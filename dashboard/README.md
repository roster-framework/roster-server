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

서버는 loopback에만 바인딩합니다. 다른 로컬 포트가 필요하면 `PORT`를 설정하세요. 왼쪽의 Home·Calls·Relations 탭에서 작업실, 호출 목록, Node/구독/호출 관계를 확인합니다. 브라우저 API는 동일 origin의 `GET /api/overview`, `/api/calls`, `/api/nodes`, `/api/relations`, `/api/conversations`, `/api/logs`를 제공합니다.

The server binds only to loopback. Set `PORT` to choose another local port. Use the Home, Calls, and Relations tabs to inspect the workspace, Call ledger, and Node/subscription/Call graph. The browser API exposes same-origin, read-only `GET /api/overview`, `/api/calls`, `/api/nodes`, `/api/relations`, `/api/conversations`, and `/api/logs`.

## 범위 · Scope

홈 화면은 Core의 준비 상태, 등록된 Node, 현재 Call을 구분해 보여줍니다. Node 등록은 실행 가능 상태나 온라인 상태를 뜻하지 않습니다. Call 상태는 Core가 관측한 최신 스냅샷이며 업무 성공을 추정하지 않습니다.

Home separates Core readiness, registered Nodes, and current Calls. Registration does not indicate that a Node is available or online. Call state is Core's latest observed snapshot and does not infer business success.

Relations draws declared subscriptions as dashed edges and explicit retained causation as solid edges. Correlation groups do not imply order. Select a Call to inspect retained state observations; gaps, conflicting reports, and expired journal cursors remain visible as insufficient evidence. 관계 화면은 선언된 구독과 관측된 원인을 구분합니다. 상관 그룹은 실행 순서를 뜻하지 않으며, 기록 누락·상충·만료는 상태 전이 근거 부족으로 표시됩니다.

부분 실패와 오래된 데이터는 표시됩니다. Core 인스턴스가 바뀌면 메모리 기록이 재시작되었을 수 있음을 알립니다. Call 입력, Result 출력, Node binding, Event payload 및 대화 본문은 브라우저에 전달하지 않습니다.

Partial failures and stale data remain visible. A changed Core instance is reported because its in-memory records may have restarted. Call inputs, Result outputs, Node bindings, Event payloads, and conversation bodies are not sent to the browser.

## Community bundle CLI · 커뮤니티 번들 CLI

Build this package with `npm run build`. The installed community package must contain `package.json` with `"roster":{"manifest":"roster-bundle.json"}` and a `roster-bundle.json` using the `roster-bundle/1` schema. Install an exact version in a separate provider workspace with `npm install --ignore-scripts --save-exact <name>@<version>`; this CLI reads its declaration but does not execute its code. Start the provider yourself, and keep its token in the Core process environment. A local bindings JSON file has the shape `{ "bindings": { "binding-ref": { "url": "http://127.0.0.1:7331", "target_id": "example", "token_env": "ROSTER_PROVIDER_TOKEN" } } }`. The value of `ROSTER_PROVIDER_TOKEN` stays outside the JSON and Git.

이 CLI는 설치된 패키지의 선언만 읽습니다. Core 명세·기존 ID·바인딩을 먼저 검증하고, 운영자가 digest를 확인해 `--apply`를 지정해야 등록합니다. 공급자 코드는 별도 신뢰·실행 경계에 있습니다.

```sh
node build/src/cli.js bundle validate --package <installed-package-dir> --bindings <bindings.json> --core http://127.0.0.1:7350
node build/src/cli.js bundle import --package <installed-package-dir> --bindings <bindings.json> --core http://127.0.0.1:7350 --apply --expected-digest <validated-sha256>
```

The CLI prints one JSON result. `planned`/`applied` exit 0; input or compatibility rejection exits 2, a started partial import exits 3, and pre-mutation communication failure exits 4. `unresolved` means the request may have reached Core; check the current Node/subscription IDs before deciding what to do next. 출력은 JSON 한 건이며, 전송 결과가 불명인 ID는 자동 재시도하거나 되돌리지 않습니다.
