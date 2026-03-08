# ORCA Single-Input Recovery Runner

[![CI](https://github.com/dhsohn/orca_auto/actions/workflows/ci.yml/badge.svg)](https://github.com/dhsohn/orca_auto/actions/workflows/ci.yml)

> ORCA 계산 실패를 사람이 새벽에 수습하지 않도록, 실패 분석, 입력 수정, 재시도, 상태 기록, 결과 리포팅까지 자동화한 Python CLI입니다.

## 무엇을 해결하나

ORCA 계산은 몇 시간에서 며칠씩 돌다가도 `SCF NOT CONVERGED`, geometry issue, TS criteria 미충족 같은 이유로 멈출 수 있습니다. 문제는 실패 자체보다 그 다음입니다.

- 어떤 입력으로 실행됐는지 추적해야 합니다.
- 출력 파일을 읽고 실패 원인을 분류해야 합니다.
- 원본 입력을 망가뜨리지 않고 보수적으로 수정해야 합니다.
- 재시도 산출물과 최종 결과를 같은 디렉터리 안에서 일관되게 남겨야 합니다.
- 긴 작업이라 중복 실행, 중간 중단, 재개 시나리오도 안전해야 합니다.

이 프로젝트는 그 운영 문제를 줄이기 위해 만들었습니다.

## 왜 어려운가

- ORCA 종료 코드는 충분한 신호가 아닙니다. 출력 텍스트를 읽어야 실제 실패 원인을 알 수 있습니다.
- TS 계산은 `terminated normally`만으로 완료가 아닙니다. 허수 진동수 개수와 IRC 조건까지 확인해야 합니다.
- 재시도 자동화는 단순 loop가 아니라, 원본 입력 보존, retry 입력 생성, geometry restart, 상태 저장이 함께 맞물려야 합니다.
- 계산이 길기 때문에 락, 원자적 상태 저장, resume 정책, background 실행 UX가 없으면 운영 중 꼬이기 쉽습니다.

## 무엇을 만들었나

- `allowed_root` 아래에서 최신 사용자 작성 `.inp`를 선택해 실행
- `.out` 분석으로 실패 원인 분류
- 보수적인 retry recipe로 `*.retryNN.inp` 생성 후 자동 재시도
- `run_state.json`, `run_report.json`, `run_report.md` 생성
- 기존 완료 `.out`가 있으면 스킵, `--force`면 재실행
- `list`, `status`, `organize`, Telegram bot까지 포함한 운영형 CLI

## 설계 판단

- 설정은 명시적으로: 더 이상 `~/orca_runs`, `~/opt/orca/orca` 같은 개인 기본값을 조용히 가정하지 않습니다.
- 책임 분리: runner, analyzer, retry engine, state store, organizer를 분리했습니다.
- 운영 안전성 우선: 락 파일, 원자적 쓰기, stale lock 회수, resume 판정을 넣었습니다.
- 복구는 보수적으로: 원본 `.inp`를 덮어쓰지 않고 retry 입력만 생성합니다.

## 복구 시나리오 예시

```text
rxn.inp 실행
  -> rxn.out 에서 SCF 실패 감지
  -> rxn.retry01.inp 생성
     - route 에 TightSCF / SlowConv 추가
     - %scf MaxIter 300 적용
     - 직전 xyz 로 geometry restart
  -> rxn.retry01.out 정상 종료
  -> run_report.md / run_report.json 생성
  -> list 에 completed, attempts=2 로 반영
```

## 검증 근거

- GitHub Actions에서 Python `3.11`, `3.12`, `3.13` 매트릭스로 검증
- 품질 게이트: `ruff`, `mypy`, `pytest --cov`
- coverage gate: `80%`
- 단위 테스트: parser, retry rules, state/lock, organize/index, Telegram handlers
- 통합 테스트: fake ORCA executable로 `run-inp -> retry -> report 생성 -> list 반영` 흐름 검증

## 빠른 시작

### 1) 설치

```bash
cd ~/orca_auto
bash scripts/bootstrap_wsl.sh
```

`bootstrap_wsl.sh`는 `.venv`를 준비하고, 템플릿 설정 파일을 복사합니다.

### 2) 설정 파일 작성

`orca_auto`는 설정 파일이 없거나 템플릿 placeholder가 남아 있으면 친절한 오류와 함께 즉시 종료합니다.

```bash
cp config/orca_auto.yaml.example config/orca_auto.yaml
```

```yaml
runtime:
  allowed_root: "/absolute/path/to/orca_runs"
  organized_root: "/absolute/path/to/orca_outputs"
  default_max_retries: 2

paths:
  orca_executable: "/absolute/path/to/orca/orca"

telegram:
  bot_token: ""
  chat_id: ""
```

메모:

- `runtime.allowed_root`와 `paths.orca_executable`은 필수입니다.
- `runtime.organized_root`를 생략하면 `allowed_root` 옆의 `orca_outputs`를 기본값으로 사용합니다.
- Windows 레거시 경로(`C:\...`, `/mnt/c/...`)는 지원하지 않습니다.

### 3) 계산 실행

```bash
./bin/orca_auto run-inp --reaction-dir '/absolute/path/to/orca_runs/sample_rxn'
```

기본은 background 실행입니다. foreground로 돌리려면:

```bash
./bin/orca_auto run-inp --reaction-dir '/absolute/path/to/orca_runs/sample_rxn' --foreground
```

### 4) 결과 확인

```bash
./bin/orca_auto status --reaction-dir '/absolute/path/to/orca_runs/sample_rxn'
./bin/orca_auto list
cat /absolute/path/to/orca_runs/sample_rxn/run_report.md
```

## 데모 출력 예시

```text
$ ./bin/orca_auto run-inp --reaction-dir '/absolute/path/to/orca_runs/sample_rxn' --foreground
status: completed
reaction_dir: /absolute/path/to/orca_runs/sample_rxn
selected_inp: /absolute/path/to/orca_runs/sample_rxn/rxn.inp
attempt_count: 2
reason: normal_termination
run_state: /absolute/path/to/orca_runs/sample_rxn/run_state.json
report_json: /absolute/path/to/orca_runs/sample_rxn/run_report.json
report_md: /absolute/path/to/orca_runs/sample_rxn/run_report.md

$ ./bin/orca_auto list --json
[
  {
    "dir": "sample_rxn",
    "status": "completed",
    "attempts": 2,
    "inp": "rxn.inp"
  }
]
```

## 주요 명령

| 명령 | 설명 |
|------|------|
| `run-inp` | 최신 `.inp` 선택 후 실행/복구/재시도 |
| `status` | 개별 반응 디렉터리 상태 확인 |
| `list` | `allowed_root` 아래 모든 run 상태 조회 |
| `organize` | 완료된 계산 결과를 `organized_root` 아래로 이동/인덱싱 |
| `bot` | Telegram long-polling bot 실행 |

자주 쓰는 옵션:

| 옵션 | 설명 | 예시 |
|------|------|------|
| `--force` | 완료된 계산도 강제 재실행 | `run-inp --force` |
| `--max-retries N` | 재시도 횟수 조정 | `run-inp --max-retries 8` |
| `--foreground` | foreground 실행 | `run-inp --foreground` |
| `--background` | background 실행 강제 | `run-inp --background` |
| `--json` | JSON 출력 | `list --json` |

## 텔레그램 봇

텔레그램에서 상태를 조회할 수 있습니다.

```bash
./bin/orca_auto bot
bash scripts/start_bot.sh restart
```

봇 명령어:

| 명령어 | 설명 |
|--------|------|
| `/list` | 전체 시뮬레이션 목록 |
| `/list running` | 실행 중인 작업만 |
| `/list completed` | 완료된 작업만 |
| `/list failed` | 실패한 작업만 |
| `/help` | 도움말 |

## 결과 정리와 인덱싱

여러 계산 결과를 한 번에 정리하려면:

```bash
./bin/orca_auto organize --root /absolute/path/to/orca_runs
./bin/orca_auto organize --root /absolute/path/to/orca_runs --apply
```

주의:

- `--root`는 설정 파일의 `runtime.allowed_root`와 정확히 같아야 합니다.
- dry-run이 기본값이고, `--apply`를 붙여야 실제 이동이 일어납니다.
- 정리 대상은 `runtime.organized_root` 아래로 이동하며 JSONL 인덱스를 함께 관리합니다.

## DFT 모니터링 계층

이 저장소에는 단일 실행기 외에, ORCA 결과를 자동 감지하고 구조화하는 라이브러리 계층도 포함되어 있습니다.

```text
파일 시스템(.out)
  -> dft_discovery
  -> orca_parser
  -> dft_index(SQLite)
  -> dft_monitor
  -> Telegram notifier / bot
```

주요 모듈:

| 모듈 | 역할 |
|------|------|
| `orca_runner.py` | ORCA subprocess 실행과 종료 처리 |
| `out_analyzer.py` | `.out`에서 완료/실패 사유 판정 |
| `attempt_engine.py` | 재시도 루프와 최종 상태 결정 |
| `state_store.py` | 상태 저장, 원자적 쓰기, 실행 락 |
| `result_organizer.py` | 완료 run 이동과 상태 동기화 |
| `dft_monitor.py` | 완료 결과 자동 감지 및 인덱싱 |
| `telegram_bot.py` | long-polling 명령 수신 |

## 프로젝트 구조

```text
core/
  launcher.py
  cli.py
  commands/
  orca_runner.py
  out_analyzer.py
  attempt_engine.py
  state_store.py
  result_organizer.py
  dft_discovery.py
  dft_index.py
  dft_monitor.py
  telegram_bot.py
  telegram_notifier.py
tests/
scripts/
config/
docs/
```

`./bin/orca_auto`는 로컬 `.venv`를 우선 사용하는 얇은 shim이고, 내부적으로는 설치형 `orca_auto`와 같은 `core.launcher`를 호출합니다.

## 테스트 실행

```bash
ruff check .
mypy
pytest --cov --cov-report=term-missing -q
```

상세 동작 규칙과 완료 판정 로직은 [REFERENCE.md](docs/REFERENCE.md)에서 다룹니다.
