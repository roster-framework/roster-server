# Roster Log Go

독립 일반 로깅 서버입니다. 배포/봇/에이전트 스트림과 원본 인용 요약·정정·보관·storage reclaim·typed usage native API를 제공합니다. 저장 자체는 MCP/Resource/Runtime에 의존하지 않으며 조직 권한과 정책 할당은 외부 조정자의 책임입니다.

서로 다른 32자 이상 ROSTER_MEMORY_TOKEN(query), ROSTER_MEMORY_WRITE_TOKEN(write/control)과 ROSTER_DATA_DIR를 설정한 뒤 실행합니다.

```sh
go run ./cmd/roster-log-server
```

기본 주소는 loopback 7341이며 원격 listener에는 TLS가 필요합니다. 쓰기·조회 자격을 에이전트/사용자에게 조직 권한으로 발급하지 않습니다. 원장 데이터·자격 파일은 저장소에 넣지 않습니다.

CLI는 `node scripts/logs.mjs`의 append/compact/features/agents/query/retention/storage 명령을 사용합니다. 원본·정정·보관은 다른 연산입니다. compact는 삭제가 아니고 VACUUM은 보안 erase가 아닙니다. Runtime의 저널 ACK와 새 Go Core의 메모리 Journal도 독립 계약입니다.

Go 검증은 Linux Docker의 `go test ./...`, `go vet ./...`와 기존 Node CLI tests를 사용합니다. 운영 원장에 테스트용 정리/회수를 실행하지 않습니다.

상세 문서: [설계](https://github.com/roster-framework/roster-docs/blob/main/roster-log-go/design.md) · [API](https://github.com/roster-framework/roster-docs/blob/main/roster-log-go/api.md) · [TODO](https://github.com/roster-framework/roster-docs/blob/main/roster-log-go/todo.md). native 지원과 옛 Core/새 Core의 실제 통합 수락을 구분합니다.
