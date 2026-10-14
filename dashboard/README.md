# Roster Dashboard

로컬 Roster Core를 관측하고 구독 선언을 관리하는 대시보드입니다. Dashboard는 `127.0.0.1:7360`, Core는 `127.0.0.1:7350`에서 실행해야 합니다.

A local dashboard for a Roster Core instance. It reads Core status, Nodes, Calls, subscriptions, conversations, and retained logs. Its only write actions create a subscription declaration or remove one with a revision check; Call submission remains a separate Core CLI action. The Dashboard uses `127.0.0.1:7360`; Core must be available at `127.0.0.1:7350`.

## 실행 · Run

Node.js 24 이상이 필요합니다. 저장소 루트에서 `dashboard/`로 이동한 뒤 다음 명령을 실행하고 `http://127.0.0.1:7360`을 여세요.

Requires Node.js 24 or newer. From the repository root, change into `dashboard/` and run these commands, then open `http://127.0.0.1:7360`.

```sh
cd dashboard
npm ci
npm test
npm run build
npm start
```

서버는 loopback에만 바인딩합니다. 다른 로컬 포트가 필요하면 `PORT`를 설정하세요. Home·Calls·Relations 탭에서 Core 상태와 Node/호출/구독 관계를 확인합니다. Relations에서는 구독 선언을 생성하거나 관측된 revision으로 제거할 수 있습니다. Call 제출은 별도 Core CLI 작업이며 대시보드가 자동 실행하지 않습니다.

The server binds only to loopback. Set `PORT` to choose another local port. Use the Home, Calls, and Relations tabs to inspect Core status and the Node/Call/subscription graph. Relations can create a subscription declaration or remove one using its observed revision. Call submission is a separate Core CLI action and is never triggered by the dashboard.

## 범위 · Scope

홈 화면은 Core의 준비 상태, 등록된 Node, 현재 Call을 구분해 보여줍니다. Node 등록은 실행 가능 상태나 온라인 상태를 뜻하지 않습니다. Call 상태는 Core가 관측한 최신 스냅샷이며 업무 성공을 추정하지 않습니다.

Home separates Core readiness, registered Nodes, and current Calls. Registration does not indicate that a Node is available or online. Call state is Core's latest observed snapshot and does not infer business success.

Relations draws declared subscriptions as dashed edges and explicit retained causation as solid edges. A declared edge is not proof of delivery; verify the resulting Event receipt and Call evidence in Core logs. Removing a subscription uses compare-and-swap revision checking and does not cancel existing Calls. Correlation groups do not imply order. Select a Call to inspect retained state observations; gaps, conflicting reports, and expired journal cursors remain visible as insufficient evidence. 관계 화면은 구독 선언과 실제 전달 근거를 구분합니다. 전달은 Core 로그의 Event receipt와 Call 근거로 확인하세요. 구독 제거는 관측 revision을 비교하며 기존 Call을 취소하지 않습니다. 상관 그룹은 실행 순서를 뜻하지 않고, 기록 누락·상충·만료는 상태 전이 근거 부족으로 표시됩니다.

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

## Deployment JSON · 배치 JSON

Use a `roster-deployment/1` file to configure existing Runtime ManagedAgents with compare-and-swap versions and then import the referenced Core bundle. `validate` performs read-only preflight and prints a digest. Review that digest, then pass it to `apply`; the apply command repeats preflight and stops before mutation if the digest changed. Prompt and bundle paths are relative to the declaration file. Runtime and Resource credentials are read by environment variable name and are never printed. This does not install providers, run package code, invoke a model, retry, or roll back. `planned`/`applied` exit 0, preflight or digest rejection exits 2, confirmed rejection/partial application exits 3, and unknown mutation outcome exits 4. Inspect `applied` and `unresolved` in the JSON result after a partial or unknown outcome.

`roster-deployment/1` 파일로 기존 Runtime ManagedAgent를 compare-and-swap version으로 설정한 뒤 참조된 Core bundle을 가져옵니다. `validate`는 변경 없이 사전 검증하고 digest를 출력합니다. digest를 확인한 뒤 `apply`에 전달하세요. `apply`도 사전 검증을 다시 수행하며 digest가 바뀌면 mutation 전에 중단합니다. prompt와 bundle 경로는 선언 파일 기준 상대 경로입니다. Runtime·Resource 자격은 환경변수 이름으로 읽고 출력하지 않습니다. 공급자를 설치하거나 패키지 코드를 실행하거나 모델을 호출하지 않으며, 자동 재시도·롤백도 없습니다. `planned`/`applied`는 종료 코드 0, 사전 검증·digest 거절은 2, 확인된 거절·부분 적용은 3, mutation 결과 불명은 4입니다. 부분 적용 또는 결과 불명 뒤에는 JSON의 `applied`와 `unresolved`를 확인하세요.

Minimal declaration example (the referenced bundle files must already exist; put the Runtime token value in the operator environment, never in this JSON):

최소 선언 예시입니다. 참조하는 bundle 파일을 미리 준비하고 Runtime token 값은 이 JSON이 아닌 운영자 환경에 설정하세요.

```json
{
  "schema": "roster-deployment/1",
  "id": "local-echo",
  "runtime": { "origin": "http://127.0.0.1:7331", "token_env": "ROSTER_TOKEN" },
  "core": { "origin": "http://127.0.0.1:7350" },
  "profiles": { "echo": { "name": "Echo Agent", "kind": "echo" } },
  "agents": [{
    "id": "echo-agent",
    "expected_version": 0,
    "profile_ref": "echo",
    "config": {},
    "skill_refs": [],
    "mcp_refs": [],
    "resource_refs": []
  }],
  "resources": [],
  "bundle": { "package_dir": "./bundle", "bindings_file": "./bindings.json" }
}
```

```sh
node build/src/cli.js deployment validate --file ../deployment.json
node build/src/cli.js deployment apply --file ../deployment.json --expected-digest <validated-sha256>
```
