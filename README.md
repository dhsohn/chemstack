# ORCA Single-Input Recovery Runner

[![CI](https://github.com/dhsohn/orca_auto/actions/workflows/ci.yml/badge.svg)](https://github.com/dhsohn/orca_auto/actions/workflows/ci.yml)

> ORCA 계산이 실패해도 자동으로 입력을 수정하고 다시 돌려주는 도구입니다.
> 밤새 돌린 계산이 SCF 수렴 실패로 멈춰있는 일, 이제 걱정하지 마세요.

---

## 빠른 시작 가이드

### 1단계: 설치

```bash
cd ~/orca_auto
bash scripts/bootstrap_wsl.sh
```

예시에서는 저장소 안의 `./bin/orca_auto`를 사용합니다.
패키지 엔트리포인트로 설치된 `orca_auto` 명령을 사용해도 동일한 launcher UX를 제공합니다.
즉 `run-inp`의 기본 백그라운드 실행, `pid`/`log` 출력, `--foreground`/`--background` 처리가 동일합니다.

### 2단계: 계산 실행

```bash
./bin/orca_auto run-inp --reaction-dir '~/orca_runs/내_반응_폴더'
```

설치형 명령을 쓰는 경우 아래처럼 실행해도 동작은 같습니다.

```bash
orca_auto run-inp --reaction-dir '~/orca_runs/내_반응_폴더'
```

`run-inp`는 기본적으로 백그라운드로 시작됩니다. 실행하면 `pid`와 `log` 경로가 출력됩니다.
터미널을 점유한 포그라운드 실행이 필요하면 `--foreground`를 추가하세요.
명시적으로 백그라운드 실행을 강제하려면 `--background`를 사용하세요.
항상 포그라운드로 동작시키려면 `export ORCA_AUTO_RUN_INP_BACKGROUND=0`을 설정하세요.

이것만 하면 됩니다! 나머지는 orca_auto가 알아서 처리합니다:
- 폴더에서 가장 최근 `.inp` 파일을 자동으로 찾아 실행
- 계산이 실패하면 입력을 보수적으로 수정하여 기본 2회 자동 재시도 (`--max-retries`로 변경 가능)
- 실행 결과를 `run_report.md`에 정리

### 3단계: 결과 확인

```bash
# 전체 시뮬레이션 목록 한눈에 보기
./bin/orca_auto list

# 실행 중인 작업만 필터
./bin/orca_auto list --filter running

# 개별 실행 상태 확인
./bin/orca_auto status --reaction-dir '~/orca_runs/내_반응_폴더'

# 또는 결과 요약 파일 직접 확인
cat ~/orca_runs/내_반응_폴더/run_report.md
```

### 4단계: 결과 정리 (선택)

여러 계산 결과를 한 번에 정리하려면 `organize`를 사용하세요.

```bash
# 기본값은 dry-run (이동하지 않고 계획만 출력)
./bin/orca_auto organize --root ~/orca_runs

# 실제로 파일/디렉터리 정리 적용
./bin/orca_auto organize --root ~/orca_runs --apply
```

주의:
- `--root`는 설정 파일의 `runtime.allowed_root`와 동일한 루트여야 합니다.
- `--root` 스캔은 해당 루트 하위 전체를 재귀적으로 확인합니다. (중첩 디렉터리 포함)
- `--apply`를 붙여야 실제 변경이 발생합니다. (`--apply` 없으면 미리보기만 수행)
- 정리 대상은 `runtime.organized_root`(기본값: `~/orca_outputs`) 아래로 이동/인덱싱됩니다.

### 자주 쓰는 옵션

| 옵션 | 설명 | 예시 |
|------|------|------|
| `--force` | 이미 완료된 계산도 강제 재실행 | `./bin/orca_auto run-inp --reaction-dir '...' --force` |
| `--max-retries N` | 재시도 횟수 변경 (기본 2회) | `./bin/orca_auto run-inp --reaction-dir '...' --max-retries 8` |
| `--json` | 결과를 JSON으로 출력 | `./bin/orca_auto run-inp --reaction-dir '...' --json` |
| `--foreground` | `run-inp`를 터미널 점유 모드로 실행 | `./bin/orca_auto run-inp --reaction-dir '...' --foreground` |
| `--background` | `run-inp`를 백그라운드 실행으로 강제 | `./bin/orca_auto run-inp --reaction-dir '...' --background` |
| `--filter` | `list`에서 상태별 필터링 | `./bin/orca_auto list --filter completed` |

### 텔레그램 봇

텔레그램에서 직접 시뮬레이션 상태를 조회할 수 있습니다.

```bash
# 봇 시작 (포그라운드)
./bin/orca_auto bot

# 백그라운드로 실행
bash scripts/start_bot.sh
```

WSL 부팅 시 자동 시작하려면 `~/.bashrc`에 추가:

```bash
~/orca_auto/scripts/start_bot.sh
```

`start_bot.sh`는 PID 파일로 중복 실행을 방지합니다.

#### 봇 명령어

| 명령어 | 설명 |
|--------|------|
| `/list` | 전체 시뮬레이션 목록 |
| `/list running` | 실행 중인 작업만 |
| `/list completed` | 완료된 작업만 |
| `/list failed` | 실패한 작업만 |
| `/help` | 도움말 |

### 자동 스케줄링 (Crontab)

매주 자동으로 정리 작업을 실행하려면:

```bash
bash scripts/install_cron.sh
```

스케줄:
- **토요일 자정**: `organize --apply` (완료된 계산을 `~/orca_outputs`로 이동)

### 작동 원리 한눈에 보기

```
.inp 파일 선택 → ORCA 실행 → 결과 분석 → 실패 시 입력 수정 → 재시도 (기본 2회, 총 최대 3회 실행)
                                         ↓
                                    성공 시 리포트 생성
```

### 테스트 실행

```bash
pytest -q
```

GitHub Actions CI에서도 `push`/`pull_request` 시 동일한 `pytest -q`가 자동 실행됩니다.

### DFT 계산 모니터링 및 자동 알림

ORCA 계산 결과를 자동으로 감지·파싱·인덱싱하여 텔레그램 봇 등 외부 서비스에서 조회할 수 있는 라이브러리 계층입니다.

#### 동작 흐름

```
파일 시스템 (ORCA .out)
    ↓
dft_discovery  ──→  완료된 .out 파일 탐색
    ↓
orca_parser    ──→  에너지·방법론·수렴 등 메타데이터 추출
    ↓
dft_index      ──→  SQLite(dft.db)에 구조화 저장
    ↓
dft_monitor    ──→  주기적 스캔으로 신규/변경 감지 → 자동 인덱싱
    ↓
텔레그램 봇    ──→  새 계산 완료 알림 / 결과 조회
```

#### 주요 모듈

| 모듈 | 역할 |
|------|------|
| `orca_parser.py` | `.out` 파일에서 계산 유형, 방법론, 에너지, 수렴 상태, 열역학 물성 등 추출 |
| `dft_discovery.py` | `orca_runs`/`orca_outputs` 하위를 재귀 탐색하여 인덱싱 대상 `.out` 선별 |
| `dft_index.py` | SQLite DB 관리 — 증분 인덱싱(해시 기반), 다차원 필터 쿼리, 통계 |
| `dft_monitor.py` | 파일 mtime 추적으로 신규/변경 감지 → 자동 파싱·인덱싱, 상태 JSON 영속화 |
| `telegram_notifier.py` | 텔레그램 봇 API로 스캔 결과 HTML 메시지 전송 (외부 의존성 없음) |

#### 텔레그램 알림 설정

`config/orca_auto.yaml`에 텔레그램 봇 정보를 추가하면 활성화됩니다.
설정 파일이 없으면 `config/orca_auto.yaml.example`을 복사하세요:

```bash
cp config/orca_auto.yaml.example config/orca_auto.yaml
```

```yaml
telegram:
  bot_token: "123456:ABC-DEF..."
  chat_id: "987654321"
```

- `bot_token`: [@BotFather](https://t.me/BotFather)에서 발급받은 봇 토큰
- `chat_id`: 알림을 받을 채팅 ID (개인 또는 그룹)
- 두 값이 모두 설정되어야 알림이 활성화됩니다

### 프로젝트 구조

```
core/
├── launcher.py           # 공통 사용자 진입점 (설치형 `orca_auto`와 동일 UX)
├── commands/             # CLI 커맨드 핸들러
│   ├── _helpers.py       # 공유 유틸 (검증, 포맷, 설정 경로)
│   ├── run_inp.py        # run-inp, status 커맨드
│   ├── list_runs.py      # list 커맨드
│   └── organize.py       # organize 커맨드
├── orca_parser.py        # ORCA 출력 파서
├── dft_discovery.py      # 완료 계산 탐색
├── dft_index.py          # SQLite 인덱스 관리
├── dft_monitor.py        # 변경 감지 및 자동 인덱싱
├── telegram_notifier.py  # 텔레그램 알림 전송
├── telegram_bot.py       # 텔레그램 봇 (long polling, 명령어 수신)
├── config.py             # 설정 로딩 및 데이터클래스
├── config_validation.py  # 설정 검증/정규화
├── lock_utils.py         # 락 파일 파싱/프로세스 생존 확인 (공유)
├── state_store.py        # 상태 저장/원자 쓰기/실행 락
├── organize_index.py     # JSONL 인덱스 관리/인덱스 락
├── attempt_engine.py     # 재시도 루프 오케스트레이션
└── ...                   # 기타 도메인 모듈
```

`./bin/orca_auto`는 로컬 `.venv`를 우선 사용하는 얇은 shim이고, 내부적으로는 설치형 `orca_auto`와 같은 `core.launcher`를 호출합니다.

> 설정, 완료 판정, 실패 복구 전략 등 상세 내용은 [상세 레퍼런스](docs/REFERENCE.md)를 참고하세요.
