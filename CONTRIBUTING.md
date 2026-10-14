# Contributing / 기여하기

Roster connects declared Node operations through one small Core. The server repository contains the Core, its dashboard, and an optional log service. Its goal is to let people and agents inspect and coordinate work through the same Call contract.

Roster는 선언된 Node operation을 작은 Core 하나로 연결합니다. 이 저장소에는 Core, 대시보드, 선택형 로그 서비스가 있습니다. 사람과 에이전트가 같은 Call 계약으로 작업을 조회하고 조정할 수 있게 하는 것이 목표입니다.

## Design principles / 설계 원칙

- A Node's registration and reachability are separate from each Call's execution state. Node 등록과 접근 가능성은 개별 Call 실행 상태와 별개입니다.
- A timeout or lost connection leaves execution uncertain; a cancellation acknowledgement does not prove termination. Timeout·연결 단절은 실행 실패의 증거가 아니며 취소 ACK도 종료 확인이 아닙니다.
- Unknown Calls require observation or operator reconciliation, never automatic reinvocation. 불명 Call은 관측 또는 운영자 조정으로 확인하며 자동 재호출하지 않습니다.
- The shared lifecycle reducer, event delivery contract, and Call model belong to Core. Providers implement their own operation semantics. 공통 전이·이벤트 전달·Call 모델은 Core가 소유하고 업무 규칙은 공급자가 구현합니다.
- A one-to-one question and a group meeting use the same conversation and Call contract. 1:1 질문과 다자 회의는 같은 대화·호출 계약을 사용합니다.

The current process uses in-memory execution state. Durable restart recovery, remote Resume, and Node availability observation remain open work. 현재 실행 상태는 메모리에 저장됩니다. 재시작 복구, 원격 Resume, Node 접근 가능성 관측은 아직 작업 중입니다.

## Issues and pull requests / 이슈와 PR

Before opening an issue, search existing issues. Describe the observable behavior, the Node/operation and Call state involved, expected behavior, and a minimal reproduction. Remove credentials, personal paths, and private payloads. 이슈를 열기 전에 기존 이슈를 검색하고 관측된 동작, 관련 Node/operation과 Call 상태, 기대 동작, 최소 재현 방법을 적어 주세요. 자격 정보·개인 경로·비공개 payload는 제거해 주세요.

For a PR, link the issue or explain the use case. State which contract or transition changes, how duplicate/out-of-order reports and cancellation races behave, and which Docker module checks you ran. Keep unrelated changes separate. PR에는 이슈 또는 사용 사례를 연결하고 계약·전이 변경, 중복/역순 보고와 취소 경합 처리, 실행한 Docker 모듈 검증을 적어 주세요. 무관한 변경은 분리해 주세요.

Usage and local setup are in this repository's [README](README.md). Public contracts are in the source and component READMEs; propose design changes in an Issue or PR. Internal design documents are maintained separately. 사용법은 README를 보고, 계약 변경 제안은 Issue나 PR로 남겨 주세요. 내부 설계 문서는 별도로 관리합니다.
