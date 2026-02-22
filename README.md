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

### 작동 원리 한눈에 보기

```
.inp 파일 선택 → ORCA 실행 → 결과 분석 → 실패 시 입력 수정 → 재시도 (최대 5회)
                                         ↓
                                    성공 시 리포트 생성
```

> 설정, 완료 판정, 실패 복구 전략 등 상세 내용은 [상세 레퍼런스](docs/REFERENCE.md)를 참고하세요.
