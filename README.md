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

To start both prepared services with one command, build the Core binary from `core/` with `go build -o roster-core.exe ./cmd/roster` and the dashboard from `dashboard/` with `npm ci && npm run build`. Create an owner-only config JSON and a separate owner-only env JSON **outside this repository**, then run from the repository root:

```sh
node scripts/launch.mjs --config ../local-config/server.json
```

The config uses `schema: "roster-launch/1"`, `data_root`, `env_file`, `core_binary`, and `dashboard_entry` (the absolute path to `dashboard/build/src/gateway.js`). The env file has the shape `{ "env": {} }` for an empty Core; add only approved Core token environment names and list them in `core_token_env` if you will import Nodes later. Core and dashboard stay on loopback ports 7350 and 7360. The launcher checks readiness, owns only its child processes, and keeps its data/lock outside Git. Its grouped startup and shutdown have been verified in Linux Docker; native Windows ACL and signal behavior remain unverified. 두 서비스를 한 번에 시작하려면 위 빌드 산출물과 저장소 밖의 설정·환경 파일을 준비하세요. 자격 값은 저장소나 설정 본문에 넣지 마세요.

In another terminal, query the same Core through its CLI. `catalog`, HTTP commands, and MCP use the same command registry. 다른 터미널에서 같은 Core를 CLI로 조회할 수 있습니다.

```sh
cd core
'{}' | go run ./cmd/roster core.status --file -
'{}' | go run ./cmd/roster nodes.list --file -
```

To register a Runtime or Resource Node, start [Roster Node Client](https://github.com/roster-framework/roster-node-client) first, then use `nodes.register` or a local Core configuration. `core/examples/local-stack.json` is a fixture: set its Node IDs, target IDs, provider URLs and `token_env` names for your own hosts. Keep credential values in the process environment, outside JSON and Git. Runtime/Resource Node를 등록하려면 먼저 Node Client를 실행하고 `nodes.register` 또는 로컬 Core 설정을 사용하세요. 예제 설정의 ID·주소·환경변수 이름은 실제 환경에 맞게 수정해야 합니다.

Call inspection uses `calls.submit`, `calls.get`, `calls.list`, `calls.inspect`, and `calls.abort`. A disconnected provider or timeout can leave a Call `unknown`; inspect the same receipt before any operator decision. Provider registration alone does not prove current reachability. 호출 조회에는 같은 CLI/HTTP/MCP 명령을 쓰며, 연결 단절·timeout은 실패 확정이 아닙니다. 등록은 현재 접속 가능성을 증명하지 않습니다.

Community packages can declare Nodes and subscriptions for an operator to import. Install an exact package version in a dedicated provider workspace with `npm install --ignore-scripts --save-exact <name>@<version>`, and run its provider separately. From this repository, build `dashboard/`, then validate and explicitly apply the bundle with the CLI below. `<bindings.json>` maps the package's `binding_ref` names to provider origins, target IDs, and **environment variable names** (`token_env`), never credential values. `validate` returns a digest; copy that value into `import`. An import may be partial, so inspect its `applied` and `unresolved` IDs before any manual follow-up. The importer never runs package code or retries Calls.

```sh
node dashboard/build/src/cli.js bundle validate --package <installed-package-dir> --bindings <bindings.json> --core http://127.0.0.1:7350
node dashboard/build/src/cli.js bundle import --package <installed-package-dir> --bindings <bindings.json> --core http://127.0.0.1:7350 --apply --expected-digest <validated-sha256>
```

커뮤니티 패키지는 선언된 Node·구독을 등록할 수 있습니다. 정확한 버전을 전용 작업 공간에 설치하고 공급자 서비스를 별도로 실행하세요. `validate`의 digest를 `import --expected-digest`에 지정해야 변경이 적용됩니다. 부분 적용 결과의 `applied`·`unresolved`를 확인한 뒤 수동으로 조정하세요. 가져오기 도구는 패키지 코드를 실행하거나 Call을 자동 재시도하지 않습니다.

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

Usage details: [Core](core/README.md), [Dashboard](dashboard/README.md), [Log](log/README.md). Contribution goals and Issue/PR guidance: [CONTRIBUTING](CONTRIBUTING.md). Internal design documents are maintained separately. 사용법은 구성요소 README에, 기여 안내는 CONTRIBUTING에 있습니다. 내부 설계 문서는 별도로 관리합니다.
