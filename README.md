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

### 2단계: 계산 실행

```bash
./bin/orca_auto run-inp --reaction-dir '~/orca_runs/내_반응_폴더'
```

이것만 하면 됩니다! 나머지는 orca_auto가 알아서 처리합니다:
- 폴더에서 가장 최근 `.inp` 파일을 자동으로 찾아 실행
- 계산이 실패하면 입력을 보수적으로 수정하여 최대 5회 자동 재시도
- 실행 결과를 `run_report.md`에 정리

### 3단계: 결과 확인

```bash
# 실행 상태 확인
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

### 5단계: 결과 품질 점검 (선택)

완료된 계산 결과의 품질을 점검합니다 (허수 진동수, SCF 수렴, 결합 길이, 스핀 오염 등).

```bash
# 단일 디렉터리 점검
./bin/orca_auto check --reaction-dir ~/orca_outputs/opt/H2/run_001 --json

# organized_root 전체 스캔 (기본)
./bin/orca_auto check --json

# 특정 루트 스캔
./bin/orca_auto check --root ~/orca_outputs --json
```

리턴코드: `0` = fail 없음, `1` = fail 존재

### 6단계: 디스크 사용량 모니터링 (선택)

디스크 사용량을 점검합니다. Telegram 알림은 `--watch` 모드에서 임계치 진입/해제 전이 시에만 전송됩니다.

```bash
# 1회 스캔
./bin/orca_auto monitor --json

# 커스텀 임계치
./bin/orca_auto monitor --threshold-gb 10 --top-n 5 --json

# 주기적 감시 (Ctrl+C로 종료)
./bin/orca_auto monitor --watch --interval-sec 60 --threshold-gb 50 --top-n 10
```

### 7단계: 불필요한 파일 정리 (선택)

정리된 계산 결과(`~/orca_outputs`)에서 불필요한 파일을 삭제하여 디스크 공간을 확보합니다.

```bash
# 기본값은 dry-run (삭제하지 않고 계획만 출력)
./bin/orca_auto cleanup --root ~/orca_outputs

# 실제로 파일 삭제 적용
./bin/orca_auto cleanup --root ~/orca_outputs --apply

# 특정 디렉터리만 대상
./bin/orca_auto cleanup --reaction-dir ~/orca_outputs/opt/H2/run_001 --apply
```

보존 파일: `.inp`, `.out`, `.xyz`, `.gbw`, `.hess`, `run_state.json`, `run_report.json`, `run_report.md`
기본 삭제 대상: `.densities`, `.engrad`, `.tmp`, `.prop`, `.scfp`, `.opt` 등

`retry*.inp`, `retry*.out`, `*_trj.xyz`까지 삭제하려면 `cleanup.remove_overrides_keep: true`를 설정하세요.

> 보존/삭제 기준은 `config/orca_auto.yaml`의 `cleanup` 섹션에서 변경할 수 있습니다.

### 자주 쓰는 옵션

| 옵션 | 설명 | 예시 |
|------|------|------|
| `--force` | 이미 완료된 계산도 강제 재실행 | `./bin/orca_auto run-inp --reaction-dir '...' --force` |
| `--max-retries N` | 재시도 횟수 변경 (기본 2회) | `./bin/orca_auto run-inp --reaction-dir '...' --max-retries 8` |
| `--json` | 결과를 JSON으로 출력 | `./bin/orca_auto run-inp --reaction-dir '...' --json` |

### Telegram 알림 받기 (선택사항)

계산 시작/완료/실패 시 Telegram으로 알림을 받을 수 있습니다.

```bash
export ORCA_AUTO_TELEGRAM_BOT_TOKEN='봇_토큰'
export ORCA_AUTO_TELEGRAM_CHAT_ID='채팅_ID'
./bin/orca_auto run-inp --reaction-dir '~/orca_runs/내_반응_폴더'
```

### 자동 스케줄링 (Crontab)

매주 자동으로 정리 작업을 실행하려면:

```bash
bash scripts/install_cron.sh
```

스케줄:
- **토요일 자정**: `organize --apply` (완료된 계산을 `~/orca_outputs`로 이동)
- **일요일 자정**: `cleanup --apply` (불필요한 파일 삭제)

각 작업 완료 후 Telegram으로 요약을 받으려면 `~/.orca_auto_env` 파일을 생성하세요:

```bash
export ORCA_AUTO_TELEGRAM_BOT_TOKEN='봇_토큰'
export ORCA_AUTO_TELEGRAM_CHAT_ID='채팅_ID'
```

### 작동 원리 한눈에 보기

```
.inp 파일 선택 → ORCA 실행 → 결과 분석 → 실패 시 입력 수정 → 재시도 (최대 5회)
                                         ↓
                                    성공 시 리포트 생성
```

### 테스트 실행

```bash
pytest -q
```

GitHub Actions CI에서도 `push`/`pull_request` 시 동일한 `pytest -q`가 자동 실행됩니다.

### 프로젝트 구조

```
core/
├── commands/             # CLI 커맨드 핸들러
│   ├── _helpers.py       # 공유 유틸 (검증, 포맷, 설정 경로)
│   ├── run_inp.py        # run-inp, status 커맨드
│   ├── organize.py       # organize 커맨드
│   ├── check.py          # check 커맨드
│   ├── monitor.py        # monitor 커맨드
│   └── cleanup.py        # cleanup 커맨드
├── geometry_checker.py   # 계산 결과 품질 점검 엔진
├── disk_monitor.py       # 디스크 사용량 스캔
├── config.py             # 설정 로딩 및 데이터클래스
├── config_validation.py  # 설정 검증/정규화
├── lock_utils.py         # 락 파일 파싱/프로세스 생존 확인 (공유)
├── notifier.py           # Telegram 알림 퍼사드/팩토리 (호환 API)
├── notifier_events.py    # 이벤트 페이로드/메시지 렌더링
├── notifier_state.py     # dedup 상태 로드/저장/정리
├── notifier_runtime.py   # 큐 오버플로우/워커/하트비트 루프
├── state_store.py        # 상태 저장/원자 쓰기/실행 락
├── organize_index.py     # JSONL 인덱스 관리/인덱스 락
├── attempt_engine.py     # 재시도 루프 오케스트레이션
├── orchestrator.py       # 하위 호환 re-export 심
└── ...                   # 기타 도메인 모듈
```

> 설정, 완료 판정, 실패 복구 전략 등 상세 내용은 [상세 레퍼런스](docs/REFERENCE.md)를 참고하세요.
