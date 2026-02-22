# ORCA 결과 정리기 설계 계획서

## Context

`orca_auto`는 계산 실행과 재시도 자동화는 제공하지만, 결과 디렉터리 구조는 사용자가 수동으로 관리한다. 계산 건수가 늘어나면 검색/비교/재현이 느려지므로 자동 정리 계층이 필요하다.

## 목표

1. 완료(`completed`)된 계산 결과만 규칙 기반으로 자동 분류한다.
2. 디렉터리명과 메타데이터를 일관된 스키마로 유지한다.
3. dry-run과 실제 적용 모드를 분리해 안전하게 운영한다.

## 비목표

1. ORCA 계산 로직 자체 변경
2. 화학적 의미 재해석
3. 외부 DB 자동 업로드

## 대상 경로

1. 입력 루트: `/home/daehyupsohn/orca_runs`
2. 정리 결과 루트: `/home/daehyupsohn/orca_outputs`
3. 메타데이터 인덱스: `/home/daehyupsohn/orca_outputs/index/`

## 분류 단위

1. 기본 정리 단위는 reaction directory 전체(기존 실행 단위)이다.
2. 대표 산출물은 이동 가능 여부와 인덱스 기록을 검증하기 위한 필수 검사 항목이다.
   - 최종 `.out`
   - 최종 `.inp`
   - `run_state.json`
   - `run_report.json`
   - `run_report.md`
3. 대표 산출물 누락 시 해당 항목은 정리 대상에서 skip하고 경고 로그를 기록한다.

## 정리 대상 판정

정리 대상은 `RunStatus.COMPLETED` 항목만으로 한정한다. 그 외 상태는 모두 skip 처리한다.

1. **정리 대상**: `run_state.json`의 `status == "completed"` 이고 `final_result`가 존재하는 경우
2. **skip**: `RunStatus.FAILED`, `RunStatus.CREATED`, `RunStatus.RUNNING`, `RunStatus.RETRYING`
3. **skip**: `run_state.json`이 없거나 파싱 실패
4. **skip**: `status == "completed"`이지만 대표 산출물(`.out`, `.inp`)이 누락된 경우 — 경고 로그 기록 후 건너뜀

## 디렉터리 네이밍 규칙

형식:

`{job_type}/{molecule_key}/{run_id}`

예시:

`ts/C8H10O2/run_20260222_101530_ab12cd34`

규칙:

1. `job_type`은 `result_organizer` 전용 분류기 `detect_job_type()`으로 결정한다.
   - `ts` ← route line에 `OptTS` 또는 `NEB-TS` 포함
   - `opt` ← route line에 `Opt` 포함 (단, `OptTS` 제외)
   - `sp` ← route line에 `SP` 또는 `Energy` 포함
   - `freq` ← route line에 `Freq`, `NumFreq`, `AnFreq` 포함
   - `other` ← 위 어디에도 해당하지 않는 경우
   - 기존 `completion_rules.detect_completion_mode()`의 route line 파싱 로직을 재사용한다.
2. `molecule_key`는 우선순위로 결정
   - 사용자 지정 태그 (`.inp` 주석 `# TAG: <name>` 또는 별도 메타파일)
   - `.inp` geometry 블록 파싱 → 분자식 조합 (예: `C 0.0 0.0 0.0` → C 원자 카운트)
     - `* xyz` 인라인 좌표: 좌표 블록 직접 파싱
     - `* xyzfile` 외부 참조: 해당 `.xyz` 파일을 읽어서 원소 파싱 (`inp_rewriter.py`의 `GEOM_HEADER_RE` 재사용)
   - reaction directory 이름에서 추론 (예: `Int1_DMSO` → `Int1_DMSO`)
   - 미확인 시 `unknown`
3. `run_id`는 `run_state.json`의 값을 사용 (형식: `run_{YYYYMMDD}_{HHMMSS}_{uuid4_hex8}`)

## 메타데이터 스키마

`index/records.jsonl` 1행 예시:

```json
{
  "run_id": "run_20260222_101530_ab12cd34",
  "reaction_dir": "/home/daehyupsohn/orca_outputs/ts/C8H10O2/run_20260222_101530_ab12cd34",
  "status": "completed",
  "analyzer_status": "completed",
  "reason": "normal_termination",
  "job_type": "ts",
  "molecule_key": "C8H10O2",
  "selected_inp": "rxn.inp",
  "last_out_path": "rxn.retry01.out",
  "attempt_count": 2,
  "completed_at": "2026-02-22T10:15:45+00:00",
  "organized_at": "2026-02-22T10:16:12+00:00",
  "organized_path": "ts/C8H10O2/run_20260222_101530_ab12cd34"
}
```

필드 설명:

1. `analyzer_status`, `reason`, `completed_at`는 `run_state.json`의 `final_result`에서 추출한다.
2. `molecule_key`는 디렉터리 네이밍 규칙에 따라 결정된 값을 기록한다.
3. `attempt_count`는 `len(run_state["attempts"])`에서 추출한다.
4. `organized_path`는 정리 결과 루트 기준 상대경로를 기록한다.
5. `selected_inp`, `last_out_path`는 `organized_path` 디렉터리 기준 상대경로로 기록한다.

## CLI 제안

기존 CLI(`run-inp`, `status`)와 동일한 subcommand 패턴을 따른다. 정리 대상은 항상 `completed` 항목만이므로 `--status` 필터는 두지 않는다.

1. `orca_auto organize --reaction-dir <dir> --dry-run`
2. `orca_auto organize --reaction-dir <dir> --apply`
3. `orca_auto organize --root <root> --apply`
4. `orca_auto organize --rebuild-index`
5. `orca_auto organize --find --run-id <run_id>`
6. `orca_auto organize --find --job-type <job_type> --limit <n>`

`--dry-run`이 기본값이며, `--apply` 없이는 파일 시스템 변경이 발생하지 않는다. `--root`는 입력 루트 일괄 스캔용이고, `--reaction-dir`은 단건 정리용이다. 두 옵션은 상호 배타적이다. `--root`는 반드시 설정의 `allowed_root`와 동일해야 한다. `organized_root`는 설정 파일(`config/orca_auto.yaml`)에서 읽는다.

구현: `cli.py`의 `build_parser()`에 `organize` subcommand를 추가하고, `orchestrator.py`에 `cmd_organize()` 핸들러를 추가한다.

## 처리 흐름

1. 입력 디렉터리 유효성 검증
2. 상태 파일 로드
3. 정리 대상 판정 — `status != "completed"` 또는 `final_result` 부재 시 skip
4. 대표 산출물 경로 결정 및 누락 검사 — 누락 시 skip + 경고
5. 대상 디렉터리명 계산 (`{job_type}/{molecule_key}/{run_id}`)
6. 사전 작업 매니페스트(`index/pending/<run_id>.json`) 기록
7. 충돌 검사
8. dry-run 출력 또는 실제 이동
9. 이동 후 `run_state.json` 경로 필드(`reaction_dir`, `selected_inp`, `final_result.last_out_path`)를 새 위치 기준으로 갱신
10. 인덱스 기록 업데이트
11. pending 매니페스트 제거 후 성공 마킹

## 충돌 처리 정책

1. 동일 `run_id`가 이미 인덱스에 있고 대상 경로가 동일하면 skip
2. 동일 `run_id`가 인덱스에 있지만 대상 경로가 다르면 `index_conflict`로 실패 처리
3. 동일 경로 점유 시 suffix 부여
   - `_v2`, `_v3`, ...

## 트랜잭션/롤백 정책

1. `--apply` 시 항목별로 `move_plan`(source, target, started_at)을 기록한 뒤 이동을 수행한다.
2. 이동은 가능한 경우 atomic rename을 사용하고, cross-device(`EXDEV`)면 `shutil.copytree`(메타데이터 보존)+디렉터리 `fsync`+`shutil.rmtree` 순서로 대체한다.
3. 항목 단위 실패 시 해당 항목의 `move_plan` 기준으로 즉시 원위치 롤백을 시도한다.
4. 롤백 실패 항목은 `index/failed_rollbacks.jsonl`에 별도 기록하고 다음 항목 처리를 계속한다.

## 인덱스 동시성 정책

1. `records.jsonl` 갱신 전 `index/index.lock`을 획득한다.
2. lock 획득 실패 시 지정된 timeout 이후 해당 배치를 실패 처리한다.
3. 인덱스 단건 append는 `open("a")` + `fsync`로 수행한다(O(1)). `--rebuild-index` 시에만 전체 재생성(O(n))을 수행한다. 기존 `state_store._atomic_write_text()`를 rebuild 시 재사용한다.
4. 인덱스 레코드 키는 `run_id`를 유니크 키로 간주한다.

## 루트 스캔 가드레일

1. 입력 루트와 정리 루트는 서로 포함 관계가 아니어야 한다.
2. `--root` 일괄 정리 시 `organized_root` 하위 경로는 스캔 대상에서 제외한다. (기존 `pathing.is_subpath()` 재사용)
3. 심볼릭 링크 순환을 방지하기 위해 기본 스캔은 symlink를 추적하지 않는다.

## 비정상 상태 파일 처리

비정상 상태는 모두 skip 처리하고 경고 로그를 기록한다.

1. `run_state.json`이 없거나 JSON 파싱 실패 → skip, `reason=state_missing_or_invalid` 로그
2. `run_state.json`은 있지만 `run_id`/`status` 핵심 필드가 누락 → skip, `reason=state_schema_invalid` 로그
3. 상태 파일과 산출물의 불일치(예: `final_result.last_out_path` 미존재) → skip, `reason=state_output_mismatch` 로그
4. `status != "completed"` → skip (정상 동작, 로그 레벨 debug)

## 안전 장치

1. `--apply` 없이 파일 시스템 변경 금지
2. 실제 이동 전 요약 출력
   - 이동 대상 수 (completed 항목)
   - skip 수 (non-completed / 산출물 누락)
   - 충돌 예상 수
3. 단일 작업 실패 시 전체 중단 대신 항목 단위 실패 기록

## 관측 지표

1. 정리된 디렉터리 수 (completed → organized)
2. skip 수 (non-completed / 산출물 누락 / 비정상 상태)
3. 충돌 건수
4. 인덱스 갱신 시간

## 테스트 계획

단위 테스트:

1. 정리 대상 판정 함수 (completed만 대상, 나머지 skip)
2. `detect_job_type()` 분류 함수
3. `molecule_key` 파싱 함수 (인라인 xyz, xyzfile 참조, 디렉터리명 추론)
4. 네이밍 규칙 함수
5. 충돌 해소 로직
6. dry-run 출력 포맷

통합 테스트:

1. completed 케이스 정리
2. non-completed 케이스 skip 검증 (failed, created, running 등)
3. 산출물 누락 케이스 skip 검증
4. 대량 디렉터리 처리(성능)

회귀 테스트:

1. 기존 `run-inp`/`status` 동작 무영향
2. 인덱스 재생성 idempotent 보장

## 구현 모듈 배치

1. `core/result_organizer.py` — 정리 대상 판정, metadata 추출, 경로 계산, `detect_job_type()`
2. `core/molecule_key.py` — `.inp`/`.xyz` 파싱 및 분자식 조합 (`inp_rewriter.GEOM_HEADER_RE` 재사용)
3. `core/organize_index.py` — JSONL 인덱스 읽기/쓰기/재생성 (`state_store._atomic_write_text()` 재사용)

재사용 대상:

- `state_store._atomic_write_text()` → 인덱스 rebuild 시 원자적 쓰기
- `state_store.load_state()` → 상태 파일 로드
- `pathing.is_subpath()` → 루트 스캔 가드레일
- `inp_rewriter.GEOM_HEADER_RE` → geometry 블록 유형 판별
- `completion_rules.detect_completion_mode()` → route line 파싱 기반

## 설정 스키마 확장

`config/orca_auto.yaml`에 `organized_root`를 추가한다:

```yaml
runtime:
  allowed_root: "/home/daehyupsohn/orca_runs"
  organized_root: "/home/daehyupsohn/orca_outputs"
  default_max_retries: 5

paths:
  orca_executable: "/home/daehyupsohn/opt/orca/orca"
```

구현:

1. `config.py`의 `RuntimeConfig` dataclass에 `organized_root: str` 필드 추가
2. 동일한 Linux 절대경로 검증 규칙 적용 (Windows 경로 거부, 상대경로 거부)
3. `organized_root`와 `allowed_root`의 포함 관계 검증 (`is_subpath` 상호 불포함 확인)
4. 기본값: `"/home/daehyupsohn/orca_outputs"`

## 단계별 구현

Phase 1:

1. `config.py` — `organized_root` 설정 필드 및 검증 추가
2. `core/result_organizer.py` — 정리 대상 판정 + `detect_job_type()` + metadata 추출기
3. `core/molecule_key.py` — 분자식 추론 함수 (인라인 xyz + xyzfile 참조)
4. 경로 계산기 추가
5. dry-run CLI 추가 (`cli.py` + `orchestrator.py`)

Phase 2:

1. apply 모드 추가
2. 충돌 처리 및 인덱스 기록 추가
3. 실패 항목 리포트 추가

Phase 3:

1. 루트 일괄 정리 모드 추가
2. 성능 개선(병렬 스캔)
3. 운영 문서/예시 정리

## 완료 기준

1. 대표 디렉터리 100건 dry-run 검증 완료 (completed 항목만 정리, 나머지 skip)
2. apply 모드에서 데이터 손실 0건
3. 인덱스 기반 검색(`--find`)이 `run_id`, `job_type` 조건에서 동작함
4. non-completed 항목이 정리 대상에 포함되지 않음을 검증
5. 롤백 드릴(강제 실패 시나리오)에서 원복 성공률 100% 확인
6. 테스트 스위트 및 스모크 테스트 통과
