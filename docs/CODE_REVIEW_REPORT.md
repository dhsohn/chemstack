# ORCA Auto 코드 리뷰 보고서

> 분석일: 2025-02-25
> 대상: 전체 코드베이스 (core/ 27모듈, tests/ 22파일, 348 테스트 전수 통과)

---

## 1. 전체 평가 요약

| 영역 | 평가 | 비고 |
|------|------|------|
| 모듈 분리 | 양호 | 리팩토링을 통해 책임이 잘 분리됨 |
| 에러 핸들링 | 보통 | 일부 silent exception swallowing 존재 |
| 테스트 커버리지 | 양호 | 348 테스트, 핵심 로직 잘 커버 |
| 타입 안전성 | 보통 | str 기반 상태 비교가 다수 |
| 설정 유연성 | 보통 | 일부 중요 임계값이 하드코딩 |
| 성능 | 보통 | 대형 분자/파일에서 비효율 가능성 |
| 보안/안전 | 양호 | atomic write, lock, allowed_root 검증 등 |

---

## 2. 설계(Architecture) 개선 사항

### 2.1 [HIGH] `orchestrator.py`의 backward-compat shim이 private 심볼 노출

**파일**: [orchestrator.py](core/orchestrator.py)

`orchestrator.py`가 `_cleanup_plan_to_dict`, `_cmd_cleanup_apply`, `_emit` 등 underscore 접두사를 가진 내부 함수들을 모두 re-export하고 있습니다. 이는 내부 구현 세부사항을 public API로 고정시켜 향후 리팩토링을 어렵게 만듭니다.

**권장**: 실제 외부에서 참조되는 심볼만 남기고 나머지 제거. 만약 외부 참조가 없다면 이 파일 자체를 deprecation 후 삭제.

---

### 2.2 [MEDIUM] Lock 획득 패턴 중복

**파일**: [state_store.py:194-259](core/state_store.py#L194-L259), [organize_index.py:234-295](core/organize_index.py#L234-L295)

`acquire_run_lock`과 `acquire_index_lock`이 거의 동일한 stale-lock 감지 로직(PID 체크, process_start_ticks 비교, 제거)을 포함하고 있습니다. `lock_utils.py`에 공통 함수(`parse_lock_info`, `is_process_alive` 등)를 이미 분리했지만, lock 획득/해제 context manager 자체도 공통화할 수 있습니다.

**권장**: `lock_utils.py`에 범용 `acquire_file_lock(lock_path, *, timeout=None)` context manager를 구현하고, 두 모듈에서 이를 호출하도록 변경.

---

### 2.3 [MEDIUM] 경로 해석 로직 중복

**파일**: [result_organizer.py:51-73](core/result_organizer.py#L51-L73) (`_resolve_existing_artifact`), [organize_index.py:96-123](core/organize_index.py#L96-L123) (`resolve_state_path`)

거의 동일한 로직: 절대경로이면 직접 시도 + reaction_dir 아래 이름만으로 시도, 상대경로이면 reaction_dir 기준으로 해석. 두 함수의 차이가 미미하므로 하나로 통합 가능합니다.

**권장**: `pathing.py`에 공통 `resolve_artifact_path(path_text, base_dir)` 함수를 두고 양쪽에서 호출.

---

### 2.4 [LOW] Retry Step 3이 no-op 빈 슬롯

**파일**: [inp_rewriter.py:28-29](core/inp_rewriter.py#L28-L29)

```python
elif step == 3:
    pass
```

Step 3이 아무 동작도 하지 않으므로, 사용자의 5번의 retry 기회 중 1회가 낭비됩니다. 의도적 예약 슬롯이라면 로그를 남기거나, 실질적인 전략(예: basis set 변경, 다른 SCF 알고리즘)을 채우는 것이 좋습니다.

**권장**:
- 실제 전략을 구현하거나
- Step 번호를 조정하여 빈 슬롯 제거 (1→2→3→4로 재번호)
- 최소한 `actions.append("step3_reserved_no_action")` 로그 추가

---

## 3. 견고성(Robustness) 개선 사항

### 3.1 [HIGH] Silent exception swallowing

여러 곳에서 `except Exception: pass`로 에러를 완전히 무시합니다:

| 위치 | 코드 |
|------|------|
| [attempt_engine.py:123-124](core/attempt_engine.py#L123-L124) | 알림 실패 시 무시 |
| [attempt_engine.py:421-422](core/attempt_engine.py#L421-L422) | 알림 실패 시 무시 |

알림 실패는 치명적이지 않지만, 반복적 실패가 발생할 경우 디버깅이 어렵습니다.

**권장**: 최소한 `logger.debug` 또는 `logger.warning`으로 에러를 기록.

---

### 3.2 [MEDIUM] `detect_completion_mode`의 FileNotFoundError 미처리

**파일**: [completion_rules.py:21](core/completion_rules.py#L21)

```python
with inp_path.open("r", encoding="utf-8", errors="ignore") as handle:
```

파일이 존재하지 않으면 `FileNotFoundError`가 발생합니다. `attempt_engine.py`에서 `current_inp.exists()` 체크 후 호출하지만, `geometry_checker.py`와 `result_organizer.py`에서는 보호 없이 직접 호출하는 경로가 있습니다.

**권장**: `detect_completion_mode` 내부에 파일 존재 체크 추가하거나, `OSError` 핸들링으로 기본값(opt 모드) 반환.

---


---

### 3.4 [LOW] `_should_keep` 에서 remove_patterns이 keep 규칙보다 우선

**파일**: [result_cleaner.py:46-60](core/result_cleaner.py#L46-L60)

```python
def _should_keep(file_path, keep_extensions, keep_filenames, remove_patterns):
    for pattern in remove_patterns:
        if fnmatch.fnmatch(name, pattern):
            return False          # remove_patterns 먼저 체크 → 삭제 결정
    if name in keep_filenames:
        return True               # keep 규칙은 나중에 체크됨
```

`remove_patterns`에 `*.inp` 같은 패턴이 잘못 추가되면, `keep_extensions`에 `.inp`가 있어도 삭제됩니다.

**권장**: `keep_filenames`/`keep_extensions` 체크를 `remove_patterns` 체크보다 먼저 수행하여 안전 장치 역할을 하도록 변경.

---

## 4. 타입 안전성(Type Safety) 개선 사항

### 4.1 [HIGH] 문자열 기반 상태 비교의 타입 안전성 부재

프로젝트 전반에서 `AnalyzerStatus` enum이 정의되어 있지만, 실제 사용은 문자열 비교가 대부분입니다:

```python
# out_analyzer.py - OutAnalysis.status가 str
OutAnalysis(status="completed", ...)

# state_machine.py - 문자열로 비교
if parsed == AnalyzerStatus.COMPLETED:

# geometry_checker.py:196
if analyzer_status == "error_scf":
```

`OutAnalysis.status`가 `str`이므로 오타("compleeted")를 타입 시스템이 잡지 못합니다.

**권장**: `OutAnalysis.status`를 `AnalyzerStatus` enum 타입으로 변경하고, 직렬화 시점에만 `.value`로 변환.

---

### 4.2 [MEDIUM] `TypedDict(total=False)` 과도 사용

**파일**: [types.py](core/types.py)

`RunState`, `RunFinalResult`, `AttemptRecord` 모두 `total=False`로 선언되어 모든 필드가 선택적입니다. 실제로 `run_id`, `status`, `attempts` 등은 항상 존재해야 하는 필수 필드입니다.

**권장**: 필수 필드는 `total=True`(기본값) 기본 클래스에, 선택 필드는 `total=False` 서브클래스에 배치:

```python
class RunStateRequired(TypedDict):
    run_id: str
    reaction_dir: str
    status: str
    attempts: List[AttemptRecord]

class RunState(RunStateRequired, total=False):
    selected_inp: str
    max_retries: int
    final_result: Optional[RunFinalResult]
```

---

## 5. 성능(Performance) 개선 사항

### 5.1 [MEDIUM] geometry_checker의 O(n^2) 이중 계산

**파일**: [geometry_checker.py:124-156](core/geometry_checker.py#L124-L156)

`_compute_pair_distances`와 `_nearest_neighbor_distances` 모두 모든 원자 쌍의 유클리드 거리를 계산합니다. `check_single`에서 두 함수 모두 호출되므로, 큰 분자(수백 원자)에서 동일한 O(n^2) 계산이 2회 수행됩니다.

**권장**: 한 번의 순회에서 pair distances와 nearest-neighbor distances를 동시에 계산하는 함수로 통합.

---

### 5.2 [LOW] TS 모드에서 출력 파일 이중 스캔

**파일**: [out_analyzer.py:205-209](core/out_analyzer.py#L205-L209)

작은 파일(< `_TS_TAIL_BYTES`)은 이미 전체가 읽혀 스캔되었지만, TS 모드에서 `_scan_ts_full_for_imag_count`가 파일 전체를 다시 line-by-line으로 읽습니다.

**권장**: 이미 전체 파일을 읽은 경우(`file_size <= tail_bytes`), 읽은 텍스트를 재사용하도록 개선.

---

### 5.3 [LOW] `load_index`가 매 조회마다 전체 JSONL 파싱

**파일**: [organize_index.py:29-55](core/organize_index.py#L29-L55)

`find_by_run_id`, `find_by_job_type` 모두 내부적으로 `load_index`를 호출하여 전체 JSONL을 파싱합니다. 인덱스가 수천 건으로 커지면 비효율적입니다.

**권장**: CLI 단일 호출 기준으로는 큰 문제가 아니지만, 향후 대규모 운영 시 인메모리 캐싱 또는 SQLite 기반 인덱스를 고려.

---

## 6. 설정(Configuration) 개선 사항

### 6.1 [MEDIUM] 사용자 특정 경로가 dataclass 기본값에 하드코딩

**파일**: [config.py:28-29](core/config.py#L28-L29), [config.py:36](core/config.py#L36)

```python
@dataclass
class RuntimeConfig:
    allowed_root: str = "/home/daehyupsohn/orca_runs"
    organized_root: str = "/home/daehyupsohn/orca_outputs"

@dataclass
class PathsConfig:
    orca_executable: str = "/home/daehyupsohn/opt/orca/orca"
```

다른 사용자가 이 프로젝트를 clone하면 기본값이 존재하지 않는 경로를 가리킵니다.

**권장**:
- 기본값을 빈 문자열 또는 sentinel 값으로 두고, 설정 파일 없이 실행 시 명확한 에러 메시지 출력
- 또는 `~` 기반 상대 경로 사용 (예: `~/orca_runs`)

---

### 6.2 [LOW] 분석 임계값이 코드에 하드코딩

**파일**: [geometry_checker.py:34-37](core/geometry_checker.py#L34-L37)

```python
_SHORT_CONTACT_THRESHOLD = 0.5
_FRAGMENTATION_NN_THRESHOLD = 3.5
_SCF_ENERGY_CHANGE_THRESHOLD = 1e-6
_SPIN_CONTAMINATION_THRESHOLD = 0.1
```

이 값들은 연구 분야에 따라 조정이 필요할 수 있으나 설정으로 노출되지 않았습니다.

**권장**: `orca_auto.yaml`에 `check` 섹션을 추가하여 임계값을 설정 가능하게 하거나, 최소한 상수를 한 곳에 모아 문서화.

---

## 7. 코드 품질(Code Quality) 개선 사항

### 7.1 [LOW] `for item in sorted_list: return item` 패턴

**파일**: [geometry_checker.py:78-81](core/geometry_checker.py#L78-L81), [geometry_checker.py:96-98](core/geometry_checker.py#L96-L98), [geometry_checker.py:107-111](core/geometry_checker.py#L107-L111)

```python
inps = sorted(reaction_dir.glob("*.inp"), key=..., reverse=True)
for inp in inps:
    return inp     # 첫 번째 요소만 반환
return None
```

가독성이 떨어지는 패턴입니다.

**권장**: `return inps[0] if inps else None`으로 간결하게 표현.

---

### 7.2 [LOW] 함수 스코프 import (순환 참조 우회)

**파일**: [organize_index.py:158-159](core/organize_index.py#L158-L159)

```python
def rebuild_index(organized_root):
    ...
    from .result_organizer import detect_job_type
    from .molecule_key import extract_molecule_key
```

순환 참조를 함수 스코프 import로 우회하고 있습니다. `organize_index`→`result_organizer`→`organize_index` 순환이 존재함을 암시합니다.

**권장**: `detect_job_type`과 `extract_molecule_key`를 별도 유틸리티 모듈로 분리하여 순환 참조를 근본적으로 해결.

---

### 7.3 [LOW] `_atomic_write_text`의 불필요한 unlink 시도

**파일**: [state_store.py:68-80](core/state_store.py#L68-L80)

`os.replace` 성공 후에도 `finally` 블록에서 이미 사라진 temp 파일을 `unlink` 시도합니다. `OSError`로 무시되지만 불필요한 시스템 콜입니다.

**권장**: `os.replace` 성공 시 `finally`의 unlink을 건너뛰도록 플래그 사용.

---

## 8. 테스트(Testing) 개선 사항

### 8.1 [MEDIUM] UTF-16 인코딩 감지 경로 테스트 부재

**파일**: [out_analyzer.py:38-51](core/out_analyzer.py#L38-L51)

`_detect_encoding`이 UTF-16 LE, UTF-16 BE, BOM 없는 UTF-16 LE를 감지하지만, 이 경로를 검증하는 테스트가 없습니다.

**권장**: UTF-16으로 인코딩된 ORCA 출력 파일 샘플로 테스트 추가.

---

### 8.2 [LOW] Cross-device move (`EXDEV`) 경로 테스트 부재

**파일**: [result_organizer.py:248-257](core/result_organizer.py#L248-L257)

`execute_move`에서 `os.rename` 실패 시 `shutil.copytree` + `shutil.rmtree` 폴백이 있지만, `EXDEV` 시나리오에 대한 테스트가 없습니다.

---

### 8.3 [LOW] `_overflow_drop` 동시성 테스트 부재

Queue overflow 상황에서의 스레드 안전성을 검증하는 멀티스레드 테스트가 없습니다.

---

## 9. 개선 우선순위 요약

| 우선순위 | 항목 | 섹션 | 난이도 |
|---------|------|------|--------|
| 1 | Silent exception에 로깅 추가 | 3.1 | 낮음 |
| 2 | `_should_keep` 에서 keep 규칙 우선 적용 | 3.4 | 낮음 |
| 3 | `OutAnalysis.status`를 enum으로 변경 | 4.1 | 중간 |
| 4 | Lock 획득 로직 공통화 | 2.2 | 중간 |
| 5 | 경로 해석 로직 통합 | 2.3 | 낮음 |
| 6 | Step 3 retry 전략 구현 또는 제거 | 2.4 | 낮음 |
| 7 | 하드코딩 기본 경로 개선 | 6.1 | 낮음 |
| 8 | geometry_checker 이중 O(n^2) 통합 | 5.1 | 중간 |
| 9 | `orchestrator.py` shim 정리 | 2.1 | 낮음 |
| 10 | `detect_completion_mode` 에러 처리 | 3.2 | 낮음 |
| 11 | TypedDict 필수/선택 필드 분리 | 4.2 | 중간 |
| 12 | UTF-16 / EXDEV 테스트 추가 | 8.1-8.2 | 낮음 |
| 14 | TS 모드 출력 이중 스캔 제거 | 5.2 | 낮음 |
| 15 | 분석 임계값 설정 노출 | 6.2 | 낮음 |

---

## 10. 긍정적 평가

- **Atomic write 패턴**: `_atomic_write_text`로 상태 파일 손상을 방지하는 견고한 구현
- **Lock-based 동시 실행 방지**: PID + process_start_ticks 기반 stale lock 감지가 정교함
- **Dry-run 기본 모드**: organize/cleanup 명령이 기본적으로 dry-run으로 동작하여 데이터 손실 방지
- **테스트 충실도**: 348개 테스트가 전수 통과하며 핵심 로직을 잘 커버
- **모듈 분리**: 리팩토링을 통해 단일 책임 원칙을 잘 따르고 있음
- **Graceful shutdown**: SIGTERM 핸들링, 프로세스 트리 종료 등 프로세스 생명주기 관리가 우수
