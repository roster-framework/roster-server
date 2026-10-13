# Roster Core

Go 1.26으로 만든 작은 중앙 Core입니다. 여러 Runtime과 Resource 서버의 Node가 선언한 operation을 같은 Call 계약으로 호출합니다. 대상 종류에 따라 상태 엔진을 나누지 않으며, 호출 상태·이벤트 전달·로그와 대화 기록을 공통 모듈에서 관리합니다.

현재는 단일 프로세스 메모리 PoC입니다. 상태와 이벤트 전달 의도는 프로세스 안에서 함께 commit하고 안정 ID로 재전달하지만, 프로세스 장애 후 복구는 아직 보장하지 않습니다. 연결 끊김·timeout·취소 ACK를 실행 종료로 간주하지 않으며, 불명 Call을 자동 재실행하지 않습니다. 원격 Resume와 IAM은 제공하지 않습니다.

## 실행

```sh
go build -o roster ./cmd/roster
./roster serve --config examples/local-stack.json
```

설정 파일 없이 시작해 `nodes.register`로 구성할 수도 있습니다. 기본 HTTP 주소는 `http://127.0.0.1:7350`, MCP 주소는 `/mcp`입니다. CLI·HTTP·MCP는 같은 command registry를 사용합니다.

```sh
printf '{}' | ./roster nodes.list --file -
curl -X POST http://127.0.0.1:7350/api/commands/nodes.list -H 'Content-Type: application/json' -d '{}'
```

`examples/local-stack.json`의 Node ID·URL·token_env를 각 서버에 맞게 지정합니다. 자격 값은 설정이나 저장소에 넣지 않고 환경변수에서 읽습니다. Runtime preset과 Resource provider 업무 규칙은 각 서버가 소유합니다.

## 검증

```sh
docker run --rm --mount type=bind,source="${PWD}",target=/src,readonly -w /src golang:1.26 go test -race -count=1 ./tests/...
```

서버를 함께 실행하는 격리 검증은 `scripts/verify-isolated.ps1`의 `-RuntimeRepository`, `-ResourceRepository`, `-WorkDirectory`, `-CacheDirectory` 인자를 사용합니다. 테스트용 자격과 데이터 디렉터리를 별도로 지정합니다.

설계와 상태 다이어그램, [API](https://github.com/roster-framework/roster-docs/blob/main/roster-core/api.md), [TODO](https://github.com/roster-framework/roster-docs/blob/main/roster-core/todo.md)는 별도 [roster-docs](https://github.com/roster-framework/roster-docs/blob/main/roster-core/design.md)에서 관리합니다. Checkpoint/restore와 self-host 검증은 미완성 WIP이며 현재 제공 범위로 표시하지 않습니다.
