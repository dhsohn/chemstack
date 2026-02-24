# ORCA Auto 상세 레퍼런스

`ORCA` 계산이 중간에 실패하거나 TS 기준을 만족하지 못할 때, 입력 파일(`.inp`)을 자동으로 보수적 수정하여 재시도하고 결과 산출물까지 정리하는 실행기입니다.

## 1) 프로젝트 목적

- 사용자가 지정한 `~/orca_runs/<reaction_dir>` 1개를 대상으로 실행
- 해당 디렉터리 안에서 가장 최근 수정된 `*.inp` 1개를 자동 선택
- 실패/중단/TS 미달 시 `*.retryNN.inp`를 생성하여 자동 재시도
- 실행 상태와 결과를 같은 디렉터리에 기록

## 2) 핵심 동작 요약

- 입력 루트 제한: 설정된 `allowed_root` 하위 디렉터리만 허용
- 대상 파일 선택: 최신 수정 `*.inp` 1개
- 기본 동작: 기존 `*.out`가 완료 상태면 스킵
- 강제 재실행: `--force` 사용 시 기존 완료 `*.out`가 있어도 재실행
- 실행 시간 제한 없음: ORCA 프로세스가 정상/비정상 종료될 때까지 대기
- 상태 파일: `run_state.json`
- 결과 리포트: `run_report.json`, `run_report.md`

## 3) 디렉터리 구조

```text
~/orca_auto
  config/orca_auto.yaml
  bin/orca_auto
  core/
    commands/              # CLI 커맨드 핸들러
      _helpers.py          # 공유 유틸 (검증, 포맷, 설정 경로)
      run_inp.py           # run-inp, status 커맨드
      organize.py          # organize 커맨드
      cleanup.py           # cleanup 커맨드
    config.py              # 설정 로딩 및 데이터클래스
    config_validation.py   # 설정 검증/정규화 함수
    lock_utils.py          # 락 파일 파싱/프로세스 생존 확인 (공유)
    notifier.py            # Telegram 알림 퍼사드/팩토리 (호환 API)
    notifier_events.py     # 이벤트 페이로드/메시지 렌더링
    notifier_state.py      # dedup 상태 로드/저장/정리
    notifier_runtime.py    # 큐 오버플로우/워커/하트비트 루프
    state_store.py         # 상태 저장/원자 쓰기/실행 락
    organize_index.py      # JSONL 인덱스 관리/인덱스 락
    attempt_engine.py      # 재시도 루프 오케스트레이션
    orchestrator.py        # 하위 호환 re-export 심
    ...                    # 기타 도메인 모듈
  scripts/*.sh / *.py
  tests/*.py
```

## 4) 요구 환경

- Linux (WSL2 또는 네이티브 Linux)
- ORCA Linux 바이너리 경로 접근 가능 (`~/opt/orca/orca`)
- ORCA 의존성: OpenMPI, BLAS/LAPACK 등
- Python 3.8+
- 입력 데이터 루트: `~/orca_runs` (ext4 파일시스템 권장)

## 5) 설치 및 초기 준비

```bash
cd ~/orca_auto
bash scripts/bootstrap_wsl.sh
```

`bootstrap_wsl.sh`는 다음을 수행합니다.

- ORCA Linux 바이너리 존재 확인
- Python venv 준비 (`.venv`)
- 의존성 설치 (`requirements.txt`)
- `orca_auto` 실행 준비

## 6) 설정 파일

설정 파일: `~/orca_auto/config/orca_auto.yaml`

기본 설정 경로 탐색 순서:

1. 환경변수 `ORCA_AUTO_CONFIG`
2. 실행 코드 기준 상대경로 `<project_root>/config/orca_auto.yaml`
3. `~/orca_auto/config/orca_auto.yaml`

```yaml
runtime:
  allowed_root: "/home/daehyupsohn/orca_runs"
  organized_root: "/home/daehyupsohn/orca_outputs"
  default_max_retries: 5

paths:
  orca_executable: "/home/daehyupsohn/opt/orca/orca"

monitoring:
  enabled: true
  telegram:
    bot_token_env: "ORCA_AUTO_TELEGRAM_BOT_TOKEN"
    chat_id_env: "ORCA_AUTO_TELEGRAM_CHAT_ID"
    timeout_sec: 5
    retry_count: 2
    retry_backoff_sec: 1.0
    retry_jitter_sec: 0.3
  delivery:
    async_enabled: true
    queue_size: 1000
    worker_flush_timeout_sec: 3
    dedup_ttl_sec: 86400
  heartbeat:
    enabled: true
    interval_sec: 1800

disk_monitor:
  threshold_gb: 50.0
  interval_sec: 300
  top_n: 10

cleanup:
  keep_extensions: [".inp", ".out", ".xyz", ".gbw", ".hess"]
  keep_filenames: ["run_state.json", "run_report.json", "run_report.md"]
  remove_patterns: ["*.retry*.inp", "*.retry*.out", "*_trj.xyz"]
```

필드 설명:

- `runtime.allowed_root`: 실행 허용 디렉터리 루트
- `runtime.organized_root`: organize/cleanup 대상 루트
- `runtime.default_max_retries`: 최대 재시도 횟수
- `paths.orca_executable`: ORCA 실행 파일 경로
- `monitoring.enabled`: Telegram 모니터링 on/off
- `monitoring.telegram.bot_token_env`: bot token을 담은 환경변수 이름
- `monitoring.telegram.chat_id_env`: chat_id를 담은 환경변수 이름
- `monitoring.delivery.async_enabled`: `false`면 동기 전송 모드
- `monitoring.heartbeat.interval_sec`: heartbeat 주기(초)
- `disk_monitor.threshold_gb`: 디스크 사용량 임계치 (GB, 기본 50.0)
- `disk_monitor.interval_sec`: watch 모드 스캔 주기 (초, 기본 300)
- `disk_monitor.top_n`: 상위 디렉터리 표시 개수 (기본 10)
- `cleanup.keep_extensions`: 기본 보존 확장자
- `cleanup.keep_filenames`: 기본 보존 파일명
- `cleanup.remove_patterns`: 우선 삭제 패턴

주의:

- `default_max_retries=5`는 재시도 횟수입니다.
- 총 실행 횟수는 `초기 1회 + 재시도 5회 = 최대 6회`입니다.
- Windows 레거시 경로(`C:\...`, `/mnt/c/...`)는 설정에서 지원하지 않습니다.
- Telegram token/chat_id 실제 값은 `config/orca_auto.yaml`에 직접 넣지 말고 환경변수로만 설정하세요.

## 7) CLI 사용법

### 7.1 실행

```bash
cd ~/orca_auto
./bin/orca_auto run-inp --reaction-dir '/home/daehyupsohn/orca_runs/Int1_DMSO' --json
```

옵션:

- `--reaction-dir` (필수): 반응 디렉터리
- `--max-retries` (선택): 최대 재시도 횟수
- `--force` (선택): 기존 완료 `*.out`가 있어도 강제 재실행
- `--json` (선택): JSON 출력

### 7.2 상태 확인

```bash
./bin/orca_auto status --reaction-dir '/home/daehyupsohn/orca_runs/Int1_DMSO' --json
```

옵션:

- `--reaction-dir` (필수)
- `--json` (선택)

### 7.3 결과 정리

```bash
./bin/orca_auto organize --root '/home/daehyupsohn/orca_runs' --json
./bin/orca_auto organize --root '/home/daehyupsohn/orca_runs' --apply
```

옵션:

- `--reaction-dir`: 단일 반응 디렉터리 정리
- `--root`: 루트 스캔 정리 (`allowed_root`와 정확히 같아야 함)
- `--root` 스캔은 하위 디렉터리를 재귀 탐색하며 `run_state.json`이 있는 완료 run을 모두 수집
- `--apply`: 실제 이동 수행 (기본은 dry-run)
- `--rebuild-index`: 인덱스 재생성
- `--find --run-id <id>` / `--find --job-type <type>`: 인덱스 검색

### 7.4 계산 결과 품질 점검

```bash
./bin/orca_auto check --reaction-dir '/home/daehyupsohn/orca_outputs/opt/H2/run_001' --json
./bin/orca_auto check --root '/home/daehyupsohn/orca_outputs' --json
./bin/orca_auto check --json
```

옵션:

- `--reaction-dir`: 단일 반응 디렉터리 점검 (`allowed_root` 또는 `organized_root` 하위)
- `--root`: 루트 스캔 점검 (`allowed_root` 또는 `organized_root`와 정확히 같아야 함)
- `--json` (선택): JSON 출력
- 둘 다 없으면 `organized_root` 기본 스캔

리턴코드: `0` = fail 없음, `1` = fail 존재

점검 항목:

- `imaginary_frequencies_opt`: opt 계산에서 허수 진동수 존재 여부 (warning)
- `ts_frequency_count`: TS 계산에서 허수 진동수 개수가 1인지 확인 (error)
- `scf_convergence`: SCF 에너지 변화량 확인 (warning/error)
- `short_contacts`: 원자쌍 거리 < 0.5 A 확인 (error)
- `fragmentation_hint`: 최근접 이웃 거리 기반 분자 분리 경고 (warning)
- `spin_contamination`: `<S**2>` 기대값 비교 (warning)

### 7.5 디스크 사용량 모니터링

```bash
./bin/orca_auto monitor --json
./bin/orca_auto monitor --threshold-gb 10 --top-n 5 --json
./bin/orca_auto monitor --watch --interval-sec 60 --threshold-gb 50 --top-n 10
```

옵션:

- `--watch`: 주기적 감시 모드 (Ctrl+C로 종료)
- `--interval-sec`: 스캔 주기 (초, 기본 300)
- `--threshold-gb`: 임계치 (GB, 기본 50.0)
- `--top-n`: 상위 디렉터리 표시 수 (기본 10)
- `--json` (선택): JSON 출력

리턴코드 (one-shot): `0` = 임계치 미만, `1` = 임계치 초과
리턴코드 (watch): `0` = 정상 종료 (KeyboardInterrupt)

임계치 기준: `allowed_root + organized_root` 합산 용량 >= `threshold_gb`

one-shot 모드: 콘솔/JSON 출력 + 리턴코드만 반환 (Telegram 전송 없음)
watch 모드 알림: 임계치 진입/해제 상태 전이 시에만 Telegram 전송 (스팸 방지)

### 7.6 불필요 파일 정리

```bash
./bin/orca_auto cleanup --root '/home/daehyupsohn/orca_outputs' --json
./bin/orca_auto cleanup --root '/home/daehyupsohn/orca_outputs' --apply
```

옵션:

- `--reaction-dir`: 단일 organized 디렉터리 정리
- `--root`: 루트 스캔 정리 (`organized_root`와 정확히 같아야 함)
- `--apply`: 실제 삭제 수행 (기본은 dry-run)

정리 정책:

- 기본 보존: `.inp`, `.out`, `.xyz`, `.gbw`, `.hess`, `run_state.json`, `run_report.json`, `run_report.md`
- 기본 삭제: `*.retry*.inp`, `*.retry*.out`, `*_trj.xyz` 및 미보존 파일
- 단, `run_state.json`에서 참조 중인 `selected_inp`, `attempts[*].inp_path/out_path`, `final_result.last_out_path`는 보존

## 8) 완료 판정 규칙

입력 route line(`! ...`)을 기준으로 모드를 자동 판정합니다.

- TS 모드: `OptTS` 또는 `NEB-TS` 포함
- Opt 모드: 그 외

TS 모드 완료 조건:

- `****ORCA TERMINATED NORMALLY****` 존재
- 허수 진동수(`-xxx cm**-1`) 개수 정확히 1개
- route line에 `IRC`가 있으면 IRC marker도 필요

Opt 모드 완료 조건:

- `****ORCA TERMINATED NORMALLY****` 존재

## 9) 실패 분류와 자동 복구

대표 상태:

- `completed`
- `error_scf`
- `error_scfgrad_abort`
- `error_multiplicity_impossible`
- `error_disk_io`
- `ts_not_found`
- `incomplete`
- `unknown_failure`

재시도 시 입력 파일 수정 순서:

1. route에 `TightSCF SlowConv` 추가 + `%scf MaxIter 300`
2. `%geom Calc_Hess true`, `Recalc_Hess 5`, `MaxIter 300`
3. 추가 recipe 없음(geometry는 공통 규칙 적용)
4. `nprocs` 절반 축소(최소 2)
5. `nprocs 1`

공통 geometry 재시작 규칙:

- 매 재시도마다 직전 시도 입력과 같은 stem의 `*.xyz` 파일을 찾아 `* xyzfile ...`로 교체
- 예: `foo.retry04.inp -> foo.retry05.inp` 생성 시 `foo.retry04.xyz`를 사용
- 직전 `*.xyz`가 없으면 디렉터리에서 최신 `*_trj.xyz/xyz`를 찾아 fallback으로 사용
- fallback 후보도 없으면 geometry 교체를 생략하고 원본 geometry 블록을 유지

원칙:

- 원본 `charge/multiplicity`는 변경하지 않습니다.
- 원본 입력 파일은 보존합니다.
- 재시도 파일명은 `<name>.retry01.inp` ... `<name>.retry05.inp`

## 10) 출력 파일 설명

실행 대상 디렉터리(`~/orca_runs/<reaction_dir>`)에 생성:

- `<stem>.out`, `<stem>.retryNN.out`
- `run_state.json`
- `run_report.json`
- `run_report.md`

`run_state.json` 주요 필드:

- `run_id`
- `reaction_dir`
- `selected_inp`
- `status`
- `attempts[]`
- `final_result`

`attempts[]` 항목:

- `index`
- `inp_path`
- `out_path`
- `return_code`
- `analyzer_status`
- `analyzer_reason`
- `markers`
- `patch_actions`
- `started_at`
- `ended_at`

## 11) 운영 가이드

- 디렉터리당 입력 파일 1개를 명확히 관리하는 것을 권장합니다.
- 여러 `*.inp`가 있으면 최신 수정 파일이 선택됩니다.
- 강제 재시작이 필요하면 `--force`를 사용하세요.
- 긴 계산은 `--json` 출력으로 상태를 기록/파싱하는 운영을 권장합니다.
- `Ctrl+C`로 중단하면 실행 중 ORCA 프로세스 트리도 함께 종료를 시도하고, 상태는 `interrupted_by_user`로 기록됩니다.
- Telegram 모니터링 사용 시 실행 전에 환경변수를 먼저 설정하세요.

```bash
export ORCA_AUTO_TELEGRAM_BOT_TOKEN='YOUR_BOT_TOKEN'
export ORCA_AUTO_TELEGRAM_CHAT_ID='YOUR_CHAT_ID'
```

## 12) 자주 발생하는 문제

1. `Reaction directory must be under allowed root`
- 원인: `--reaction-dir`가 `allowed_root` 밖
- 조치: `config/orca_auto.yaml`의 `allowed_root` 확인

2. `Reaction directory not found`
- 원인: 경로 문자열/인용 문제
- 조치: 경로를 작은따옴표로 감싸 사용

예:

```bash
./bin/orca_auto run-inp --reaction-dir '/home/daehyupsohn/orca_runs/my_case'
```

3. `State file not found`
- 원인: 아직 해당 디렉터리에서 `run-inp`를 실행하지 않음
- 조치: 먼저 `run-inp` 실행

4. `error_multiplicity_impossible`
- 원인: 전자수와 다중도 조합 불일치
- 조치: 본 도구는 보수 정책으로 charge/multiplicity를 자동 변경하지 않으므로 입력을 직접 수정 후 재실행

## 13) 마이그레이션 유틸리티

Linux 전환 시 사용할 수 있는 스크립트:

- `scripts/preflight_check.sh`: Cutover 사전 점검 (프로세스, lock, 상태, 디스크)
- `scripts/audit_input_path_literals.py`: `.inp` 파일 내 Windows 경로 검출
- `scripts/validate_runtime_config.py`: 설정 유효성 검증

## 14) 테스트

```bash
cd ~/orca_auto
pytest -q
```

## 15) 권장 워크플로우

1. 입력 디렉터리 준비 (`~/orca_runs/<case>`)
2. `run-inp` 실행
3. `status`로 상태 확인
4. `run_report.md`로 최종 요약 검토
5. 필요 시 `--force`로 재실행
