# 로스터 서버

[English](README.md) | [한국어](README.ko.md)

중앙 Go Core와 로컬 대시보드, 선택형 로그 서비스를 실행합니다. Core는 공급자에 중립적인 Node를 등록하고 Call을 받아 결과를 관측하며 명시 구독 선언을 통해 이벤트를 전달합니다. 대시보드에서는 구독 선언을 만들고 현재 revision을 사용해 제거할 수 있습니다. Call 제출은 Core CLI에서 별도로 수행합니다.

![Roster Server 구조: Core가 Runtime·Resource Node와 연결되고 로컬 대시보드는 Core 조회와 구독 관리를 담당합니다](docs/assets/server-architecture.svg)

![호출 상태 흐름: 접수, 실행, 대기, 취소 요청, 불명, 조사, 종료 확인](docs/assets/call-state.svg)

## 로컬 실행

필요 도구는 Go 1.26, Node.js 24, npm입니다. 저장소 루트에서 각 서비스를 별도 터미널로 실행하세요.

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

<http://127.0.0.1:7360>을 여세요. Core는 `127.0.0.1:7350`에서 실행되며 대시보드 gateway가 서버 쪽에서 Core 데이터를 조회합니다. Relations 화면에서 관측된 revision으로 구독을 제거할 수 있습니다. Call 제출은 별도 Core CLI 작업입니다.

## 준비된 서비스 함께 시작하기

`core/`에서 `go build -o roster-core.exe ./cmd/roster`로 Core를 빌드하고, `dashboard/`에서 `npm ci && npm run build`로 대시보드를 빌드하세요. 저장소 **밖에** 권한을 제한한 설정 JSON과 환경 JSON을 만들고 저장소 루트에서 실행합니다.

```sh
node scripts/launch.mjs --config ../local-config/server.json
```

설정에는 `schema: "roster-launch/1"`, `data_root`, `env_file`, `core_binary`, `dashboard_entry`(절대 경로인 `dashboard/build/src/gateway.js`)를 지정합니다. 선택 항목 `authority_file`은 저장소 밖의 소유자 전용 정책 JSON 경로이며 launcher가 Core에 전달합니다. 빈 Core의 env 파일 형식은 `{ "env": {} }`입니다. 자격을 사용하는 Node를 가져올 때는 허용된 Core 토큰 환경변수를 `core_token_env`에 등록하세요. 실제 값은 외부 env 파일에만 둡니다. Core와 대시보드는 loopback의 7350·7360 포트에 바인딩합니다. Launcher는 준비 상태를 확인하고 자신이 실행한 자식 프로세스만 관리하며 데이터와 잠금 파일은 Git 밖에 둡니다. 그룹 실행·종료는 Linux Docker에서 검증했으며 Windows ACL·signal 동작은 아직 검증하지 않았습니다.

## Node와 Call 조회

CLI, HTTP, MCP는 같은 Core command registry를 사용합니다. `core/`에서 다음 명령으로 상태와 Node를 확인할 수 있습니다.

```sh
printf '%s\n' '{}' | go run ./cmd/roster core.status --file -
printf '%s\n' '{}' | go run ./cmd/roster nodes.list --file -
```

Runtime 또는 Resource Node를 등록하기 전에 [Roster Node Client](https://github.com/roster-framework/roster-node-client)를 실행하세요. `nodes.register` 또는 로컬 Core 설정을 사용할 수 있습니다. `core/examples/local-stack.json`은 예제이며 Node ID, target ID, 공급자 URL과 `token_env` 이름을 실제 환경에 맞춰야 합니다. 자격 값은 Core 프로세스 환경에만 두고 Node binding·manifest·Git에는 넣지 마세요.

Call은 `calls.submit`, `calls.get`, `calls.list`, `calls.inspect`, `calls.abort`로 제출·조회합니다. `--authority`/`authority_file`을 생략하면 Core는 빈 fail-closed 정책으로 시작합니다. readiness와 조회는 되지만 Call 제출과 사용자의 Abort는 거절됩니다. 주체, 대상 Node, action, operation을 정확히 허용하세요. 최소 local 권한 예시는 다음과 같습니다.

```json
{
  "schema": "roster-authority/1",
  "grants": [
    { "principal": { "namespace": "operators", "id": "local" }, "action": "invoke", "target": { "namespace": "nodes", "id": "runtime" }, "operations": ["run"] },
    { "principal": { "namespace": "operators", "id": "local" }, "action": "abort", "target": { "namespace": "nodes", "id": "runtime" }, "operations": ["run"] }
  ]
}
```

정책 파일에는 자격을 넣지 마세요. 연결 단절이나 timeout 뒤에는 Call이 `unknown`일 수 있으므로 같은 receipt를 확인한 뒤 조치하세요. 공급자 등록만으로 현재 연결 가능성이 증명되지는 않습니다. 구독 선언은 이후 이벤트 경로를 설정할 뿐 전달 성공을 증명하지 않습니다. Core 로그의 Event receipt와 생성된 Call 근거를 확인하세요. 구독을 제거해도 이미 생성된 Call은 취소되지 않습니다.

## 스케줄링 설정

`serve --config <file>`에 전달하는 Core JSON의 최상위에 다음 조각을 추가하세요. 같은 파일의 `nodes`에 ID가 `runtime`인 Node를 선언해야 하며, 실제 ID가 다르면 아래 ID도 바꾸세요. Node 전체 설정이 아닌 스케줄링 조각입니다.

```json
{
  "scheduling": {
    "global_waiting": 128,
    "global_possible": 16,
    "aging_after_seconds": 30,
    "targets": [
      { "node_id": "runtime", "weight": 2, "waiting": 32, "possible": 4, "active": 2 }
    ]
  }
}
```

설정을 생략하면 전체 waiting 256·possible 64·aging 기준 30초이며, 각 Node는 weight 1·waiting 64·possible 4·기본 worker 8개에서 active 2입니다. 전체 또는 worker 상한을 낮추면 생략한 기본값도 그 상한에 맞춰 줄어듭니다. 존재하지 않거나 중복된 target ID와 명시적으로 잘못 지정한 값은 시작을 거절합니다. 정책은 작업 시작 전에 검증하며 재시작할 때까지 고정됩니다.

Core는 처리 가능한 Node 사이에 가중 선택 기회를 배분합니다. Node 안에서는 처리 가능한 Call 중 aging 기준을 넘긴 Call을 최초 접수 순으로 먼저 고르고, 그 외에는 이른 deadline 순으로 고릅니다. weight는 선택 횟수의 비율이며 provider 실행시간의 비율이 아닙니다. `unknown`이나 취소 ACK만 받은 Call은 종료가 확인될 때까지 `possible`을 점유합니다. `active`는 실제 반환할 때까지 준비·Invoke 중인 로컬 worker 수를 셉니다. 상한이 차면 전송이 지연될 수 있으며 비협조적인 provider를 선점할 수는 없습니다. `core.status`의 `scheduling`에서 waiting/possible/physical 수, 가장 오래된 대기 시간과 활동 중인 target별 차단 이유를 확인하세요.

## 커뮤니티 bundle 가져오기

커뮤니티 패키지는 Node와 구독을 선언해 운영자가 명시적으로 가져올 수 있습니다. 전용 공급자 작업 공간에서 정확한 버전을 설치하고 install script 없이 공급자를 별도로 실행하세요.

```sh
npm install --ignore-scripts --save-exact <name>@<version>
```

`dashboard/`를 빌드한 뒤 저장소 루트에서 패키지를 검증하고 명시적으로 적용합니다. `<bindings.json>`은 `binding_ref`를 공급자 주소, target ID, **환경변수 이름**(`token_env`)에 연결하며 자격 값은 포함하지 않습니다. `validate`의 digest를 `import`에 전달하세요. 일부만 적용될 수 있으니 수동 조치 전 `applied`와 `unresolved`를 확인합니다. 가져오기 도구는 패키지 코드를 실행하거나 Call을 재시도하지 않습니다.

```sh
node dashboard/build/src/cli.js bundle validate --package <installed-package-dir> --bindings <bindings.json> --core http://127.0.0.1:7350
node dashboard/build/src/cli.js bundle import --package <installed-package-dir> --bindings <bindings.json> --core http://127.0.0.1:7350 --apply --expected-digest <validated-sha256>
```

## Runtime agent와 Core bundle 구성

배치 CLI는 Runtime ManagedAgent version, 기존 skill/MCP/Resource 참조를 먼저 확인한 뒤 Runtime CAS 설정과 참조된 Core bundle을 적용합니다. 공급자를 설치하거나 모델을 호출하지 않습니다. `roster-deployment/1` JSON 예시와 명령은 한국어·영어 [Dashboard 안내](dashboard/README.md)를 참고하세요.

```sh
node dashboard/build/src/cli.js deployment validate --file <deployment.json>
node dashboard/build/src/cli.js deployment apply --file <deployment.json> --expected-digest <validated-sha256>
```

## 검증

`core/`와 `log/`에서 각각 Docker 모듈 검사를 실행합니다.

```sh
docker run --rm -v "$PWD:/src:ro" -w /src golang:1.26.10 go test ./...
```

대시보드는 `dashboard/`에서 `npm test`를 실행합니다. 실제 Runtime·Resource HTTP, 구독, 1:1 응답과 그룹 대화를 모델 호출 없이 검사하려면 `../roster-node-client/runtime`을 먼저 빌드한 다음 저장소 루트에서 다음 격리 Docker fixture를 실행하세요.

```sh
pwsh -File core/scripts/verify-isolated.ps1
```

구성요소 사용법: [Core](core/README.md), [Dashboard](dashboard/README.md), [Log](log/README.md). 기여 안내: [CONTRIBUTING](CONTRIBUTING.md).
