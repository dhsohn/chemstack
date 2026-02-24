# Plan: `check` & `monitor` 명령 추가 (보완본)

작성일: 2026-02-24

## 0) 목표

orca_auto에 다음 두 기능을 추가한다.

1. `check`: 완료된 계산 결과의 품질 점검
2. `monitor`: 디스크 사용량 점검 + Telegram 알림

원칙:
- 기존 CLI 패턴(`run-inp`, `organize`, `cleanup`)과 일관성 유지
- 표준 라이브러리만 사용 (추가 의존성 없음)
- 실패 시 fail-open(알림 실패가 본 작업을 중단시키지 않음)

---

## 1) 기존 초안 대비 보완 포인트

1. CLI 옵션명 통일
- `--dir` 대신 기존 패턴과 맞춰 `--reaction-dir` 사용

2. 점검 대상 경계 명확화
- `check`는 `run_state.json`의 `status == completed`인 디렉터리만 유효 대상으로 간주
- 미완료/상태파일 누락은 skip reason으로 분리

3. 입력 파일 탐색 경로 명시
- `detect_completion_mode()` 호출을 위해 inp 경로를 우선순위로 확보:
  - `state.selected_inp`
  - `<last_out_path와 같은 stem>.inp`
  - 디렉터리 내 최신 `.inp`

4. 결합 길이 검사 오탐 방지
- 모든 원자쌍의 `> 5.0A` 경고는 비결합 원자쌍 때문에 오탐이 많음
- `짧은 거리(<0.5A)`는 전수 검사 에러
- `긴 거리`는 각 원자의 최근접 이웃 거리 기반 경고로 변경

5. monitor 알림 스팸 방지
- watch 모드에서 매 루프 전송 금지
- 임계치 "진입/해제" 상태 전이 시에만 Telegram 전송

6. 설정 fallback 누락 방지
- monitoring 비활성화 fallback 재생성 시 `disk_monitor` 필드도 유지

7. 검증 기준 최신화
- 테스트 명령은 `pytest -q` 기준으로 작성 (`pytest.ini` 사용)

---

## 2) Phase 1: `check` 명령

### 2-1. `core/geometry_checker.py` 생성

핵심 검사 모듈을 추가한다.

데이터 구조:

```python
@dataclass
class CheckItem:
    check_name: str
    severity: str  # "ok" | "warning" | "error"
    message: str
    details: Dict[str, Any]

@dataclass
class CheckResult:
    reaction_dir: str
    run_id: str
    job_type: str  # "opt" | "ts"
    overall: str   # "pass" | "warn" | "fail"
    checks: List[CheckItem]

@dataclass
class CheckSkipReason:
    reaction_dir: str
    reason: str
```

핵심 함수:
- `check_single(reaction_dir: Path) -> Tuple[Optional[CheckResult], Optional[CheckSkipReason]]`
- `check_root_scan(root: Path) -> Tuple[List[CheckResult], List[CheckSkipReason]]`

내부 헬퍼:
- `_resolve_inp_path(state: Dict[str, Any], reaction_dir: Path, out_path: Path) -> Optional[Path]`
- `_find_out_file(reaction_dir: Path, state: Dict[str, Any]) -> Optional[Path]`
- `_find_xyz_file(reaction_dir: Path, out_path: Path) -> Optional[Path]`
- `_parse_xyz_atoms(xyz_path: Path) -> List[Tuple[str, float, float, float]]`
- `_compute_pair_distances(atoms) -> List[Tuple[int, int, float]]`
- `_nearest_neighbor_distances(atoms) -> List[float]`

검사 항목:
- `_check_imaginary_frequencies_opt()`
  - opt에서 허수 진동수 존재 시 warning
- `_check_ts_frequency_count()`
  - ts에서 허수 진동수 개수가 1이 아니면 error
- `_check_scf_convergence()`
  - 마지막 에너지 변화량 파싱 실패 시 warning
  - 파싱 성공 + `|delta_E| > 1e-6`이면 warning
  - analyzer가 `error_scf`면 error
- `_check_short_contacts()`
  - 원자쌍 거리 `< 0.5A` 존재 시 error
- `_check_fragmentation_hint()`
  - 각 원자의 최근접 이웃 거리 중 최대값이 임계치 초과 시 warning
- `_check_spin_contamination()`
  - `<S**2>`와 기대값 `S(S+1)` 비교, 차이 > 0.1이면 warning

정규식(유연 파싱):

```python
S2_RE = re.compile(r"Expectation value of <S\*\*2>\s*:\s*([-+]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)")
SCF_ENERGY_CHANGE_RE = re.compile(r"Last Energy change\s+\.\.\.\s+([-+]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)")
MULT_RE = re.compile(r"Multiplicity\s+Mult\s*\.\.\.\.\s*(\d+)")
XYZ_ATOM_RE = re.compile(r"^\s*([A-Z][a-z]?)\s+([-+]?\d+(?:\.\d+)?)\s+([-+]?\d+(?:\.\d+)?)\s+([-+]?\d+(?:\.\d+)?)\s*$")
```

재사용:
- `core/out_analyzer.py`: `analyze_output()`, `NEG_FREQ_RE`, `_detect_encoding()`, `_read_tail()`
- `core/completion_rules.py`: `detect_completion_mode()`
- `core/state_store.py`: `load_state()`

skip reason 예시:
- `state_missing_or_invalid`
- `not_completed`
- `output_missing`
- `inp_missing_for_mode_detection`
- `xyz_missing`

### 2-2. `core/commands/_helpers.py` 수정

validator 추가:

```python
def _validate_check_reaction_dir(cfg: AppConfig, reaction_dir_raw: str) -> Path:
    # allowed_root 또는 organized_root 하위 허용


def _validate_check_root_dir(cfg: AppConfig, root_raw: str) -> Path:
    # allowed_root 또는 organized_root와 정확히 일치
```

### 2-3. `core/commands/check.py` 생성

동작:
- `--reaction-dir`와 `--root` 상호배제
- 둘 다 없으면 `root = cfg.runtime.organized_root` 기본 스캔
- 단일 점검 또는 root 스캔 실행
- plain/json 출력

리턴코드:
- `0`: fail 없음, 런타임 오류 없음
- `1`: fail 존재 또는 스캔/검증 오류 존재

출력 payload(요약):

```json
{
  "action": "scan",
  "checked": 12,
  "skipped": 3,
  "failed": 1,
  "warned": 4,
  "results": [...],
  "skip_reasons": [...]
}
```

### 2-4. `core/cli.py` 수정

- import: `from .commands.check import cmd_check as _cmd_check`
- wrapper 추가
- `check` 서브커맨드 추가:
  - `--reaction-dir`
  - `--root`
  - `--json`
- `command_map`에 `"check": cmd_check` 등록

### 2-5. 테스트

신규:
- `tests/test_geometry_checker.py`
- `tests/test_check_cli.py`

보강:
- `tests/test_command_helpers.py`에 check validator 케이스 추가

핵심 케이스:
- completed opt/ts 정상
- state 누락/깨짐
- out/xyz 누락
- mutually exclusive 옵션 검증
- allowed/organized 루트 경계 검증

---

## 3) Phase 2: `monitor` 명령

### 3-1. `core/config.py` 수정

설정 dataclass 추가:

```python
@dataclass
class DiskMonitorConfig:
    threshold_gb: float = 50.0
    interval_sec: int = 300
    top_n: int = 10
```

`AppConfig` 필드 추가:

```python
disk_monitor: DiskMonitorConfig = field(default_factory=DiskMonitorConfig)
```

`load_config()`에서 `disk_monitor` 파싱 후 `AppConfig`에 주입.

주의:
- monitoring 설정이 invalid여서 `MonitoringConfig(enabled=False)`로 fallback할 때도
  `disk_monitor=cfg.disk_monitor` 유지.

### 3-2. `core/config_validation.py` 수정

```python
def _validate_disk_monitor_config(dm: Any) -> None:
    if dm.threshold_gb <= 0:
        raise ValueError(...)
    if dm.interval_sec < 10:
        raise ValueError(...)
    if not (1 <= dm.top_n <= 100):
        raise ValueError(...)
```

`load_config()`에서 `_validate_disk_monitor_config(cfg.disk_monitor)` 호출.

### 3-3. `core/disk_monitor.py` 생성

데이터 구조:

```python
@dataclass
class DirUsage:
    path: str
    size_bytes: int

@dataclass
class FilesystemInfo:
    total_bytes: int
    used_bytes: int
    free_bytes: int
    usage_percent: float

@dataclass
class DiskReport:
    allowed_root: str
    allowed_root_bytes: int
    organized_root: str
    organized_root_bytes: int
    combined_bytes: int
    threshold_gb: float
    threshold_exceeded: bool
    top_dirs: List[DirUsage]
    filesystem: Optional[FilesystemInfo]
    timestamp: str
```

함수:
- `_dir_size(path: Path) -> int`
  - 재귀 합산, `OSError`/권한 오류 무시, 심볼릭 링크는 따라가지 않음
- `_top_subdirs(root: Path, limit: int) -> List[DirUsage]`
- `_get_filesystem_info(path: Path) -> Optional[FilesystemInfo]`
- `scan_disk_usage(allowed_root, organized_root, threshold_gb, top_n) -> DiskReport`

임계치 기준:
- `combined_gb = (allowed_root_bytes + organized_root_bytes) / 1024**3`
- `combined_gb >= threshold_gb`이면 `threshold_exceeded=True`

### 3-4. `core/notifier_events.py` 수정

이벤트 추가:

```python
EVT_DISK_THRESHOLD = "disk_threshold"
EVT_DISK_RECOVERED = "disk_recovered"
```

함수 추가:
- `event_disk_threshold(...)`
- `event_disk_recovered(...)`

`render_message()` 분기 추가:
- threshold 초과 메시지
- threshold 해제(recovered) 메시지

### 3-5. `core/commands/monitor.py` 생성

동작:
- one-shot: 1회 스캔 후 출력
- watch: 주기 스캔

옵션 우선순위:
- CLI 인자 우선
- 미지정 시 `cfg.disk_monitor` 사용

watch 알림 정책:
- under -> exceeded 전이 시 1회 알림
- exceeded -> under 전이 시 1회 복구 알림
- 상태 유지 중에는 재알림 없음

리턴코드:
- one-shot: 임계치 초과 시 `1`, 아니면 `0`
- watch: 정상 종료(`KeyboardInterrupt`) 시 `0`
- 설정/인자 오류는 `1`

### 3-6. `core/cli.py` 및 설정 파일 수정

`monitor` 서브커맨드 추가:
- `--watch`
- `--interval-sec`
- `--threshold-gb`
- `--top-n`
- `--json`

`config/orca_auto.yaml`에 `disk_monitor` 기본값 추가:

```yaml
disk_monitor:
  threshold_gb: 50.0
  interval_sec: 300
  top_n: 10
```

### 3-7. 테스트

신규:
- `tests/test_disk_monitor.py`
- `tests/test_monitor_cli.py`

보강:
- `tests/test_config_validation.py` (disk_monitor 검증 추가)
- `tests/test_notifier.py` 또는 notifier event 테스트 (새 이벤트 메시지)

핵심 케이스:
- threshold 초과/미만 판단
- top_n 정렬/제한
- watch 모드 전이 알림 1회성 보장
- Telegram send 함수 patch 검증

---

## 4) 예상 변경 파일

신규:
- `core/geometry_checker.py`
- `core/commands/check.py`
- `core/disk_monitor.py`
- `core/commands/monitor.py`
- `tests/test_geometry_checker.py`
- `tests/test_check_cli.py`
- `tests/test_disk_monitor.py`
- `tests/test_monitor_cli.py`

수정:
- `core/cli.py`
- `core/commands/_helpers.py`
- `core/config.py`
- `core/config_validation.py`
- `core/notifier_events.py`
- `config/orca_auto.yaml`
- `tests/test_command_helpers.py`
- `tests/test_config_validation.py`
- `tests/test_notifier.py` (또는 동등 이벤트 테스트 파일)
- `README.md`
- `docs/REFERENCE.md`

---

## 5) 구현 순서

1. `core/commands/_helpers.py`에 check validator 추가
2. `core/geometry_checker.py` + `tests/test_geometry_checker.py`
3. `core/commands/check.py` + `core/cli.py` + `tests/test_check_cli.py`
4. `core/config.py` + `core/config_validation.py`에 disk_monitor 추가
5. `core/disk_monitor.py` + `tests/test_disk_monitor.py`
6. `core/notifier_events.py` 이벤트 확장 + 관련 테스트
7. `core/commands/monitor.py` + `core/cli.py` + `tests/test_monitor_cli.py`
8. `config/orca_auto.yaml`, `README.md`, `docs/REFERENCE.md` 업데이트

---

## 6) 검증 방법

```bash
# 전체 테스트
pytest -q

# check 수동 점검
./bin/orca_auto check --reaction-dir ~/orca_outputs/opt/H2/run_001 --json
./bin/orca_auto check --root ~/orca_outputs --json

# monitor one-shot
./bin/orca_auto monitor --json
./bin/orca_auto monitor --threshold-gb 10 --top-n 5 --json

# monitor watch
./bin/orca_auto monitor --watch --interval-sec 60 --threshold-gb 50 --top-n 10
```

---

## 7) 완료 기준 (Definition of Done)

- `check`, `monitor` 명령이 CLI에 등록되고 정상 동작한다.
- 신규/수정 테스트가 `pytest -q`에서 통과한다.
- 기존 `run-inp`, `organize`, `cleanup` 동작 회귀가 없다.
- watch 모드에서 임계치 유지 구간의 반복 Telegram 스팸이 발생하지 않는다.
- README/REFERENCE와 실제 CLI 옵션/출력이 일치한다.
