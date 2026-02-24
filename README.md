# ORCA Single-Input Recovery Runner

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
- `--apply`를 붙여야 실제 변경이 발생합니다. (`--apply` 없으면 미리보기만 수행)
- 정리 대상은 `runtime.organized_root`(기본값: `~/orca_outputs`) 아래로 이동/인덱싱됩니다.

### 5단계: 불필요한 파일 정리 (선택)

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
삭제 대상: `.densities`, `.engrad`, `.tmp`, `.prop`, `.scfp`, `.opt`, `retry*.inp`, `retry*.out`, `*_trj.xyz` 등

> 보존/삭제 기준은 `config/orca_auto.yaml`의 `cleanup` 섹션에서 변경할 수 있습니다.

### 자주 쓰는 옵션

| 옵션 | 설명 | 예시 |
|------|------|------|
| `--force` | 이미 완료된 계산도 강제 재실행 | `./bin/orca_auto run-inp --reaction-dir '...' --force` |
| `--max-retries N` | 재시도 횟수 변경 (기본 5회) | `./bin/orca_auto run-inp --reaction-dir '...' --max-retries 8` |
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

> 설정, 완료 판정, 실패 복구 전략 등 상세 내용은 [상세 레퍼런스](docs/REFERENCE.md)를 참고하세요.
