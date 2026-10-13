# Roster Server / 로스터 서버

Run a central Go Core with a local dashboard. The Core calls declared Node operations through one Call contract; the dashboard shows registered Nodes, observed Call states, and read-only relations. An optional log service lives in `log/`.

중앙 Go Core와 로컬 대시보드를 실행합니다. Core는 선언된 Node operation을 공통 Call 계약으로 호출하고, 대시보드는 등록된 Node·관측된 Call 상태·읽기 전용 관계를 보여 줍니다. 선택형 로그 서비스는 `log/`에 있습니다.

## Run locally / 로컬 실행

Requirements: Go 1.26, Node.js 24, npm. Run each block in a separate terminal from this repository. 필요 도구는 Go 1.26, Node.js 24, npm입니다. 각 블록을 별도 터미널에서 실행하세요.

```sh
cd core
go run ./cmd/roster serve
```

```sh
cd dashboard
npm ci
npm run build
npm start
```

Open <http://127.0.0.1:7360>. The Core listens on `127.0.0.1:7350`; the dashboard gateway reads it on the server side and shows a local, read-only view. 처음에는 등록된 Node가 없는 화면이 정상입니다. 대시보드는 Core를 서버 쪽에서 조회하며 로컬 읽기 전용 화면을 제공합니다.

In another terminal, query the same Core through its CLI. `catalog`, HTTP commands, and MCP use the same command registry. 다른 터미널에서 같은 Core를 CLI로 조회할 수 있습니다.

```sh
cd core
'{}' | go run ./cmd/roster core.status --file -
'{}' | go run ./cmd/roster nodes.list --file -
```

To register a Runtime or Resource Node, start [Roster Node Client](https://github.com/roster-framework/roster-node-client) first, then use `nodes.register` or a local Core configuration. `core/examples/local-stack.json` is a fixture: set its Node IDs, target IDs, provider URLs and `token_env` names for your own hosts. Keep credential values in the process environment, outside JSON and Git. Runtime/Resource Node를 등록하려면 먼저 Node Client를 실행하고 `nodes.register` 또는 로컬 Core 설정을 사용하세요. 예제 설정의 ID·주소·환경변수 이름은 실제 환경에 맞게 수정해야 합니다.

Call inspection uses `calls.submit`, `calls.get`, `calls.list`, `calls.inspect`, and `calls.abort`. A disconnected provider or timeout can leave a Call `unknown`; inspect the same receipt before any operator decision. Provider registration alone does not prove current reachability. 호출 조회에는 같은 CLI/HTTP/MCP 명령을 쓰며, 연결 단절·timeout은 실패 확정이 아닙니다. 등록은 현재 접속 가능성을 증명하지 않습니다.

## Verify / 검증

Run module checks inside Docker from `core/` and `log/` separately. Docker에서 구성요소별로 검증합니다.

```sh
docker run --rm -v "$PWD:/src:ro" -w /src golang:1.26 go test ./...
```

For the dashboard, run `npm test` from `dashboard/`. To exercise real Runtime and Resource HTTP, subscriptions, a one-to-one reply, and a group conversation without a model call, first build `../roster-node-client/runtime`, then run this isolated Docker fixture from the server repository root:

```sh
pwsh -File core/scripts/verify-isolated.ps1
```

대시보드는 `dashboard/`의 `npm test`로 검증합니다. 위 스크립트는 실제 Runtime·Resource 연결과 대화 흐름을 Docker에서 검증하며 모델 호출은 하지 않습니다.

Usage details: [Core](core/README.md), [Dashboard](dashboard/README.md), [Log](log/README.md). Design/API/state diagrams: [roster-docs](https://github.com/roster-framework/roster-docs). Contribution goals and Issue/PR guidance: [CONTRIBUTING](CONTRIBUTING.md). 사용 상세는 구성요소 README에, 설계·API·상태 다이어그램은 별도 문서 저장소에 둡니다.
