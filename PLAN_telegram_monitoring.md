# ORCA Telegram 모니터링 계획서

## Context

장시간 계산은 실패/완료 시점을 실시간으로 확인하기 어렵다. `orca_auto` 실행 상태를 메시지로 전달하면 운영자가 즉시 대응할 수 있다.

## 목표

1. 실행 시작/완료/실패 이벤트를 Telegram으로 전송한다.
2. 주기 상태 요약(heartbeat)을 지원한다.
3. 알림 실패가 본 실행을 중단시키지 않도록 분리한다.

## 비목표

1. Telegram 외 메신저 통합
2. 계산 제어(원격 중단/재시작)
3. 결과 파일 전송 자동화

## 구성

1. `core/notifier.py`
   - 이벤트 payload 생성
   - 전송 결과 로깅
2. `core/telegram_client.py`
   - Bot API 호출 래퍼
3. `config/orca_auto.yaml` 확장
   - 모니터링 설정

## 설정 초안

```yaml
monitoring:
  enabled: true
  telegram:
    bot_token_env: "ORCA_AUTO_TELEGRAM_BOT_TOKEN"
    chat_id_env: "ORCA_AUTO_TELEGRAM_CHAT_ID"
    timeout_sec: 5
    retry_count: 2
    retry_backoff_sec: 1
    retry_jitter_sec: 0.3
  delivery:
    async_enabled: true
    queue_size: 1000
    worker_flush_timeout_sec: 3
    dedup_ttl_sec: 86400
  heartbeat:
    enabled: true
    interval_sec: 1800
```

원칙:

1. 토큰/채널 ID는 환경변수에서만 읽는다.
2. 설정 파일에 민감정보를 저장하지 않는다.
3. 모니터링 비활성화 시 기존 실행 경로와 동일해야 한다.
4. 모니터링 설정 오류/환경변수 누락은 fail-open으로 처리한다.
   - 경고 로그를 남기고 알림만 비활성화한다.
   - `run-inp` 실행 흐름은 중단하지 않는다.
5. Telegram 전송은 기본 `parse_mode`를 사용하지 않는 plain text로 고정한다.
6. 설정값 범위 검증을 강제한다.
   - `timeout_sec`: 1~30
   - `retry_count`: 0~5
   - `queue_size`: 100~10000
   - `heartbeat.interval_sec`: 60 이상
7. `chat_id`는 숫자형(문자열 허용)으로 검증하고, 잘못된 형식이면 notifier를 비활성화한다.

## 이벤트 스키마

공통 필드:

1. `event_type`
2. `run_id`
3. `reaction_dir`
4. `selected_inp`
5. `timestamp`

이벤트 종류:

1. `run_started`
2. `attempt_completed`
3. `run_completed`
4. `run_failed`
5. `run_interrupted`
6. `heartbeat`

`event_id` 규칙:

1. `run_started`: `{run_id}:run_started`
2. `attempt_completed`: `{run_id}:attempt_completed:{attempt_index}`
3. `run_completed`/`run_failed`/`run_interrupted`: `{run_id}:{event_type}`
4. `heartbeat`: `{run_id}:heartbeat:{bucket_ts}`

## 메시지 포맷

시작:

`[orca_auto] started | run_id=... | dir=...`

완료:

`[orca_auto] completed | run_id=... | attempts=... | reason=...`

실패:

`[orca_auto] failed | run_id=... | status=... | reason=...`

heartbeat:

`[orca_auto] heartbeat | run_id=... | status=... | attempts=... | elapsed_sec=...`

## 동작 정책

1. 전송 실패 시 최대 `retry_count`만 재시도
2. 최종 실패는 stderr + 로그 파일에 기록
3. 알림 실패 때문에 계산 상태를 실패로 바꾸지 않음
4. 동일 이벤트 중복 전송 방지를 위해 `event_id` 사용
5. Telegram `429` 수신 시 `retry_after`를 우선 적용해 재시도한다.
6. Telegram `5xx`/timeout은 exponential backoff + jitter로 재시도한다.
7. 클라이언트 에러(`4xx`, 단 `429` 제외)는 비재시도 실패로 처리한다.

## 이벤트 일관성 규칙

1. `attempt_completed`는 `save_state()` 성공 이후에만 발행한다.
2. 최종 이벤트(`run_completed`/`run_failed`/`run_interrupted`)는 `final_result` 기준으로 1회만 발행한다.
3. 동일 `run_id`에 terminal 이벤트는 하나만 허용한다.
4. 이벤트 타임스탬프는 모두 UTC ISO8601로 통일한다.
5. `heartbeat`는 terminal 이벤트 발행 이후 즉시 중단한다.
6. 큐는 FIFO를 유지하되, 우선순위 드롭 정책은 별도로 적용한다.

## 전송 아키텍처

1. `notify()`는 비동기 큐 enqueue만 수행하고 즉시 반환한다.
2. 별도 worker 스레드가 Telegram API 전송과 재시도를 담당한다.
3. 큐가 가득 찬 경우 최신 heartbeat를 우선 드롭하고 상태 전환 이벤트(`run_*`)를 우선 보존한다.
4. 프로세스 종료 시 worker를 `join(timeout=worker_flush_timeout_sec)`으로 정리한다.
5. 중복 방지 상태는 `reaction_dir/.notify_state.json`에 저장해 재시작 후에도 dedup을 유지한다.
6. worker 스레드 비정상 종료 감지 시 notifier를 fail-open으로 비활성화하고 계산은 계속 진행한다.
7. 큐 포화 시 드롭 우선순위를 고정한다.
   - 1순위 드롭: `heartbeat`
   - 2순위 드롭: 오래된 `attempt_completed`
   - 보존 대상: `run_started`, terminal 이벤트(`run_completed`/`run_failed`/`run_interrupted`)

## dedup 상태 파일 정책

1. `reaction_dir/.notify_state.json` 쓰기는 atomic write + `fsync`를 사용한다.
2. 동시 접근 보호를 위해 `reaction_dir/.notify_state.lock`을 사용한다.
3. dedup 상태 파일 파싱 실패 시 `.corrupt.<timestamp>`로 백업 후 새 파일로 재생성한다.
4. dedup TTL 경과 이벤트는 주기적으로 정리(compaction)한다.

## 보안 및 메시지 안전성

1. 로그/예외에 bot token을 절대 출력하지 않는다.
2. 메시지 텍스트에 특수문자가 포함되어도 깨지지 않도록 plain text 전송을 기본값으로 유지한다.
3. 메시지 크기 상한(예: 3500자)을 두고 초과 시 축약한다.
4. 경로 정보(`reaction_dir`)는 운영 정책에 따라 마스킹 또는 상대경로화 옵션을 둔다.

## 구현 포인트

1. `run_started` — `orchestrator.py`의 `cmd_run_inp()` 진입 직후 발행
2. `attempt_completed` — `attempt_engine.py`의 attempt loop 내부, 각 시도 종료 후 `save_state()` 직후 발행
3. `run_completed` / `run_failed` — `attempt_engine.finalize_and_emit()` 내부에서 최종 이벤트 발행
4. `run_interrupted` — `attempt_engine.finalize_and_emit()`에서 terminal 이벤트를 단일 발행한다. `KeyboardInterrupt` 경로는 이벤트 타입 override만 전달하고 별도 선발행하지 않는다.
5. `existing_out_completed` fast-path — `orchestrator.py`에서 finalize 전에 `run_completed` 이벤트를 동일하게 발행한다.
6. `heartbeat` — daemon 스레드로 구현한다.
   - 장점: 별도 프로세스 불필요, 실행 중 자동 종료
   - 단점: GIL 영향 가능하지만 I/O 위주이므로 무시 가능
   - cron 방식은 실행 상태 공유가 어려워 비채택
7. 종료 훅에서 heartbeat 스레드와 delivery worker를 순서대로 중지한다.
8. `delivery.async_enabled=false`면 queue/worker 없이 동기 전송 모드로 동작한다.

## 주입 방식

기존 아키텍처에서 `attempt_engine.run_attempts()`는 `emit` 콜백을 주입받는 패턴을 사용한다. 동일하게 `notify` 콜백을 추가 인자로 전달한다.

```python
def run_attempts(
    ...
    notify: Callable[[Dict[str, Any]], None] | None = None,
    ...
)
```

`notify`가 `None`이면 알림을 건너뛴다. 이렇게 하면 모니터링 비활성화 시 기존 동작과 완전히 동일하다.

`monitoring.enabled=true`여도 notifier 초기화 실패 시 `notify=None`으로 폴백한다.

## 테스트 계획

단위 테스트:

1. 메시지 렌더링
2. 전송 재시도 로직
3. 환경변수 누락 처리
4. 비활성화 모드 패스스루
5. `event_id` 생성/중복 제거
6. `429 retry_after` 처리
7. 큐 overflow 시 heartbeat drop 우선 정책
8. terminal 이벤트 단일성 보장(중복 발행 방지)
9. dedup 상태 파일 손상 복구
10. worker 비정상 종료 시 fail-open 동작
11. 긴 메시지 축약/잘림 처리

통합 테스트:

1. 가짜 HTTP 서버를 통한 전송 성공/실패 시나리오
2. `run-inp` 완료/실패/중단 이벤트 발생 검증
3. heartbeat 주기 검증
4. `existing_out_completed` 경로 이벤트 검증
5. `KeyboardInterrupt` 경로에서 `run_interrupted` + 상태 finalize 보장 검증
6. 종료 시 worker flush/join 검증
7. 네트워크 단절/timeout 지속 상황에서 계산 진행 보장 검증
8. 재시작 후 dedup 상태 유지 및 중복 전송 방지 검증

## 운영 체크리스트

1. 토큰/채널 ID 환경변수 설정
2. 모니터링 활성화 확인
3. 테스트 채널로 스모크 전송
4. 알림 실패 로그 알람 연결
5. Bot의 대상 채널 쓰기 권한 확인
6. 긴급 비활성화 스위치(`monitoring.enabled=false`) 동작 확인

## 단계별 구현

Phase 1:

1. `core/telegram_client.py` — Bot API 호출 래퍼 + 재시도 로직
2. `core/notifier.py` — 이벤트 payload 생성 + notify 콜백 팩토리
3. `config.py` — `monitoring` 설정 섹션 로딩/검증 추가 (범위 검증 포함)
4. dedup 상태 파일 원자적 저장/복구 유틸리티 추가
5. 단위 테스트 (메시지 렌더링, 재시도, 환경변수 누락, dedup 손상 복구)

Phase 2:

1. `orchestrator.py` — `run_started` 이벤트 훅 연결
2. `attempt_engine.py` — `notify` 콜백 주입, `attempt_completed`/최종 이벤트 발행
3. heartbeat daemon 스레드 구현
4. worker 생존 감시 + 큐 포화 드롭 우선순위 구현
5. 통합 테스트 (가짜 HTTP 서버 + 장애 시나리오)

Phase 3:

1. canary 적용 + 알림 품질 점검
2. 운영 문서 추가

## 완료 기준

1. 주요 실행 이벤트가 누락 없이 전송됨
2. 알림 장애가 계산 실행 결과에 영향 없음
3. 재시작 후 동일 `event_id` 중복 전송률 0%
4. terminal 이벤트 중복 발행 0건
5. 큐 포화 시에도 terminal 이벤트 손실 0건
6. 테스트 스위트와 운영 스모크 테스트 통과
