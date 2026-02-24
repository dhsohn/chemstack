# ORCA Auto 리팩토링 계획서

작성일: 2026-02-24
대상 브랜치: `main`

## 1) 목표

- 대규모 패치 이후 증가한 복잡도를 낮추고, 기능 추가 시 회귀 위험을 줄인다.
- `run-inp`, `organize`, `cleanup`의 책임 경계를 명확히 분리한다.
- 운영 자동화(cron/monitoring)와 도메인 로직의 결합도를 낮춘다.
- 동작 변경 없이 구조를 개선하고, 이후 기능 확장을 위한 안정된 기반을 만든다.

## 2) 현재 진단 요약

핵심 대형 모듈(라인 수):

- `core/orchestrator.py`: 698
- `core/notifier.py`: 602
- `core/attempt_engine.py`: 467
- `core/organize_index.py`: 382
- `core/result_organizer.py`: 365
- `core/state_store.py`: 345
- `core/config.py`: 293

주요 구조 문제:

- `orchestrator`가 CLI 입출력, 검증, 비즈니스 규칙, Telegram 요약 전송까지 담당.
- `organize/cleanup`의 공통 패턴(검증, plan/apply, 요약 출력)이 중복됨.
- notifier/telegram 경로가 `run-inp` 이벤트 알림과 배치 요약 알림으로 분산되어 경계가 불명확.
- 실행 환경에 따라 `PYTHONPATH=.`가 필요한 테스트 실행 방식이 DX 리스크를 유발.

## 3) 리팩토링 원칙

- 동작 보존 우선: 의미 변경 없이 모듈 분리부터 진행.
- 작은 PR/커밋 단위: 각 단계는 테스트 통과 가능한 최소 변경으로 나눈다.
- 안전 게이트 고정: 각 단계 종료 시 전체 테스트 통과(`PYTHONPATH=. pytest -q`).
- I/O 경계 분리: 파일시스템/네트워크 의존성은 어댑터 레이어로 캡슐화.

## 4) 목표 아키텍처

권장 모듈 경계:

- `core/commands/`
- `run_inp.py`, `status.py`, `organize.py`, `cleanup.py`
- 각 커맨드별 인자 해석, 유스케이스 호출, 출력 포맷 처리
- `core/services/`
- `run_service.py`, `organize_service.py`, `cleanup_service.py`
- 도메인 오케스트레이션(검증 + plan/apply + 요약 데이터 생성)
- `core/domain/`
- 상태/판정/정리 규칙(순수 로직 중심)
- `core/infra/`
- state/index 저장소, Telegram 클라이언트, 파일 이동/삭제 어댑터

`core/orchestrator.py`는 최종적으로 얇은 호환 계층(또는 제거)으로 축소.

## 5) 단계별 실행 계획

### Phase 0. 안전망 고정 (반나절)

- 테스트 실행 표준화
- `pytest.ini` 추가 또는 테스트 부트스트랩으로 `core` import 경로 고정
- CI/로컬 동일 명령으로 실행되도록 정리
- 커맨드 스모크 테스트 3종 고정
- `run-inp`, `organize --apply`, `cleanup --apply`

완료 기준:

- `pytest -q` 단독 실행 성공.

### Phase 1. orchestrator 분해 (1~2일)

- `cmd_run_inp`, `cmd_status`, `cmd_organize`, `cmd_cleanup`를 커맨드 모듈로 분리.
- 공통 유틸 분리:
- 경로 검증 함수군(`_validate_*`)
- 공통 출력 함수(`_emit*`)
- 바이트 포맷 함수(`_human_bytes`)
- `core/cli.py`는 커맨드 모듈 직접 import하도록 정리.

완료 기준:

- `core/orchestrator.py` 250라인 이하.
- 기존 CLI 테스트 전부 통과.

### Phase 2. organize/cleanup 공통 파이프라인 추상화 (1~2일)

- 공통 실행 골격 도입:
- `plan -> dry_run_emit`
- `plan -> apply -> summary_emit`
- 요약/실패 집계 타입 공통화 (`dataclass` 기반 DTO).
- guardrail 검증 로직을 서비스 레벨로 이동.

완료 기준:

- organize/cleanup 중복 코드 30% 이상 축소.
- `tests/test_organize_cli.py`, `tests/test_cleanup_cli.py` 전부 통과.

### Phase 3. 알림 경계 재정리 (1일)

- 배치 요약 Telegram 전송을 notifier 체계와 일원화.
- `orchestrator`의 `_send_summary_telegram` 제거, notifier의 별도 채널 API로 대체.
- 알림 실패 fail-open 정책을 단일 경로로 통합.

완료 기준:

- Telegram 관련 설정 검증 경로가 1곳으로 통일.
- organize/cleanup 요약 알림 테스트 추가 후 통과.

### Phase 4. config/state/index 계층 정리 (1~2일)

- `config.py`를 파싱/정규화/검증 함수로 분리.
- `state_store.py`, `organize_index.py`의 락/원자쓰기 공통 유틸 추출.
- 하드코딩된 정책 상수는 설정 가능한 값과 고정 규칙을 명확히 구분.

완료 기준:

- `config.py` 200라인 이하.
- 저장소 계층 테스트 유지 + 신규 회귀 테스트 통과.

### Phase 5. 문서/운영 절차 동기화 (반나절)

- `README.md`, `docs/REFERENCE.md`를 새 모듈 경계/운영 명령 기준으로 갱신.
- 운영자가 확인할 체크리스트 문서 추가:
- 배포 전 테스트
- cron 설치/검증
- 장애 대응(락 파일/중단 복구)

완료 기준:

- 문서 예시 커맨드 실제 실행 검증 완료.

## 6) 테스트 전략

필수 테스트 세트:

- 전체: `PYTHONPATH=. pytest -q`
- 고위험 영역 집중:
- `tests/test_cli.py`
- `tests/test_attempt_engine.py`
- `tests/test_organize_cli.py`
- `tests/test_cleanup_cli.py`
- `tests/test_notifier.py`

추가할 테스트:

- 요약 알림(organize/cleanup) 경로 단위 테스트
- 경로 검증 유틸 분리 후 파라미터화 테스트
- 공통 파이프라인 오류 집계(부분 실패/롤백 실패) 테스트

## 7) 리스크 및 대응

- 리스크: 모듈 이동 중 import 순환/깨짐
- 대응: 단계별로 모듈 이동 후 즉시 테스트, 순환 import는 인터페이스 모듈로 분리
- 리스크: 출력 포맷 변경으로 운영 스크립트 영향
- 대응: 기존 JSON/plain text key를 호환 유지, 변경 시 명시적 deprecation
- 리스크: notifier 리팩토링 중 알림 누락
- 대응: 이벤트 ID/dedup 보존 테스트 추가, fail-open 정책 유지

## 8) 실행 순서 제안

1. Phase 0 + Phase 1을 먼저 진행해 코드 경계 안정화
2. Phase 2에서 organize/cleanup 공통화
3. Phase 3에서 알림 경로 통합
4. Phase 4에서 config/store 정리
5. Phase 5 문서 동기화로 마무리

## 9) 완료 정의 (Definition of Done)

- 전체 테스트 안정 통과.
- 대형 파일 축소:
- `orchestrator.py` 250라인 이하
- `notifier.py` 400라인 이하
- `config.py` 200라인 이하
- 기능 회귀 없음:
- run/organize/cleanup/notifier 기존 계약 유지
- 문서와 실제 명령/설정이 일치.
