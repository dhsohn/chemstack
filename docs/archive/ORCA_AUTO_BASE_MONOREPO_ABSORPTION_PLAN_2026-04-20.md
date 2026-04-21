# ORCA Auto Base Monorepo Absorption Plan

> Historical note: this plan documents the absorption strategy that led to the current monorepo layout. For the live package map and import rules, use `src/chemstack/*` and see [../DEVELOPMENT.md](../DEVELOPMENT.md).

Date: 2026-04-20
Updated: 2026-04-20

## 0. 문서 목적

이 문서는 현재 가장 완성도가 높은 `orca_auto`를 기준 저장소로 삼아,
아래 프로젝트들을 하나의 모노레포와 하나의 Python 패키지 namespace로
통합하는 실행 계획을 정리한다.

대상:

- `chem_core`
- `chem_flow`
- `xtb_auto`
- `crest_auto`
- `orca_auto`

이 문서는 기존
[MCP_WORKFLOW_MIGRATION_PLAN.md](MCP_WORKFLOW_MIGRATION_PLAN.md)
위에 올라가는 상위 계획이다.

역할 분리:

- `MCP_WORKFLOW_MIGRATION_PLAN.md`
  - `orca_auto` 자체를 sibling-app 친화적으로 정렬하는 내부 구조 계획
- 이 문서
  - 그 정렬된 `orca_auto`를 베이스 레포로 삼아 전체 스택을 흡수하는 계획

## 1. 핵심 결론

`orca_auto`를 모노레포의 시작점으로 사용하는 방향은 매우 합리적이다.

이유:

- 현재 다섯 프로젝트 중 운영 완성도가 가장 높다
- CLI/worker/config/test 구조가 가장 성숙하다
- 다른 앱들이 실제로 `orca_auto`를 많이 참고해 만들어졌다
- 따라서 저장소 운영 기준, 품질 기준, 배포 기준을 `orca_auto` 쪽에 맞추는 편이 자연스럽다

다만 중요한 제약이 있다.

- 다른 프로젝트들을 `orca_auto 레포`에 흡수하는 것은 좋다
- 하지만 다른 프로젝트들을 `orca_auto 패키지` 안으로 흡수하는 것은 좋지 않다

권장 최종 형태:

- 레포 베이스: `orca_auto`
- 최종 패키지 namespace: `chemstack`
- ORCA 코드는 `chemstack.orca`
- shared core는 `chemstack.core`
- workflow는 `chemstack.flow`
- xTB/CREST는 `chemstack.xtb`, `chemstack.crest`

즉:

- 저장소 기준 중심축은 `orca_auto`
- 아키텍처 기준 중심축은 `chemstack`

## 2. 왜 `orca_auto`를 베이스 레포로 삼는가

## 2.1 장점

### 운영 완성도

`orca_auto`는 현재 다음 축이 가장 잘 정리되어 있다.

- operator-facing CLI
- foreground worker 운영 모델
- retry ladder와 state/report 관리
- organize 흐름
- monitoring 및 Telegram 연계
- 풍부한 테스트
- 비교적 정돈된 문서

관련 위치:

- [README.md](/home/daehyupsohn/orca_auto/README.md)
- [../REFERENCE.md](../REFERENCE.md)
- [core/](/home/daehyupsohn/orca_auto/core)
- [tests/](/home/daehyupsohn/orca_auto/tests)

### 다른 앱들의 사실상 템플릿

현재 `xtb_auto`, `crest_auto`는 공개 surface와 운영 방식에서 `orca_auto`의 패턴을 많이 따르고 있다.

이 말은 곧:

- repo layout
- config policy
- CLI naming
- worker lifecycle
- organize/reindex/summary command shape

를 `orca_auto` 기준으로 통합하기 쉽다는 뜻이다.

### 베이스 레포 선택 비용 감소

새 빈 레포를 만드는 것보다 `orca_auto`를 출발점으로 삼으면 좋은 점:

- 기존 CI와 문서 자산 활용 가능
- 가장 많은 운영 지식을 담은 레포에서 시작 가능
- 사용자 입장에서 "가장 익숙한 프로젝트"를 기준으로 재구성 가능

## 2.2 주의점

`orca_auto`의 강점이 곧 전체 플랫폼의 패키지 이름이 되어야 한다는 뜻은 아니다.

특히 현재 [core/](/home/daehyupsohn/orca_auto/core) 안에는 ORCA 전용 구현이 많다.

예:

- ORCA parser
- ORCA retry engine
- ORCA-specific state machine
- ORCA input rewrite logic
- ORCA result organizer

그래서 `orca_auto`를 베이스 레포로 택하더라도, 최종 namespace를
`orca_auto.*`로 통일하는 것은 장기적으로 경계를 흐릴 가능성이 크다.

## 3. 최종 목표 구조

권장 최종 구조:

```text
orca_auto/                      # 기존 repo를 모노레포 루트로 확장
  pyproject.toml
  README.md
  src/
    chemstack/
      __init__.py
      core/
      flow/
      orca/
      xtb/
      crest/
  tests/
    core/
    flow/
    orca/
    xtb/
    crest/
    integration/
  docs/
  scripts/
  deploy/
  examples/
```

중요:

- Git repo 이름이 당분간 `orca_auto`여도 괜찮다
- 하지만 Python import namespace는 `chemstack`으로 가는 것이 좋다

예시 import:

```python
from chemstack.core.queue import enqueue
from chemstack.flow.orchestration import advance
from chemstack.orca.runner import run_orca_job
from chemstack.xtb.runner import run_xtb_job
from chemstack.crest.runner import run_crest_job
```

## 4. 반드시 유지해야 하는 원칙

### 4.1 repo와 package를 분리해서 생각한다

- repo 기준으로는 `orca_auto`가 중심
- package 기준으로는 `chemstack`이 중심

### 4.2 계층을 유지한다

허용:

- `chemstack.flow -> chemstack.core`
- `chemstack.xtb -> chemstack.core`
- `chemstack.crest -> chemstack.core`
- `chemstack.orca -> chemstack.core`

금지:

- `chemstack.core -> flow/xtb/crest/orca`

### 4.3 CLI 이름은 당분간 유지한다

초기 전환 단계에서는 사용자-facing 명령을 유지하는 편이 좋다.

예:

```toml
[project.scripts]
orca_auto = "chemstack.orca.cli:main"
xtb_auto = "chemstack.xtb.cli:main"
crest_auto = "chemstack.crest.cli:main"
chem_flow = "chemstack.flow.cli:main"
```

### 4.4 `orca_auto/core` 전체를 `chemstack.core`로 옮기지 않는다

현재 `core/`는 이름만 core일 뿐, 실제로는 ORCA 전용 구현 묶음이 크다.
따라서:

- 진짜 공용 인프라만 `chemstack.core`로 이동
- ORCA 전용 로직은 `chemstack.orca`에 남긴다

## 5. 현재 상태에서의 해석

## 5.1 이미 통합 친화적인 축

다음 세 프로젝트는 이미 `chem_core` 위에서 비교적 깔끔하게 정리되어 있다.

- `chem_flow`
- `xtb_auto`
- `crest_auto`

즉, 이 셋은 큰 방향에서 이미 다음 구조를 암시하고 있다.

- app shell
- shared core dependency
- engine/workflow-specific module

## 5.2 `orca_auto`는 가장 성숙하지만 가장 독특하다

`orca_auto`는 가장 완성도가 높지만, 동시에 현재 전체 스택에서 구조적으로 가장 독립적이다.

대표 차이:

- 자체 `core/` 패키지를 크게 유지
- `chem_core`와 완전 통합되지 않은 compatibility 경계가 아직 존재
- tests 다수가 `from core...`를 전제로 함

즉, `orca_auto`는 "베이스 레포"로는 최적이지만 "그 구조를 그대로 모두에게 복제하는 것"은 최적이 아니다.

## 5.3 기존 ORCA 정렬 계획이 선행 조건이다

현재
[MCP_WORKFLOW_MIGRATION_PLAN.md](MCP_WORKFLOW_MIGRATION_PLAN.md)
은 이미 다음 방향을 담고 있다.

- `orca_auto` package realignment
- `chem_core` config alignment
- queue/admission alignment
- tracking/job_locations facade 정리
- `chem_flow` cutover

이 문서는 모노레포 흡수 전에 ORCA 내부 경계를 조금 더 깔끔하게 만들라는 강한 시그널로 읽을 수 있다.

따라서 전체 모노레포 흡수는 이 정렬 작업을 무시하고 바로 들어가면 안 된다.

## 6. 권장 실행 순서

전체 전략은 두 단계로 나눈다.

1. `orca_auto`를 "모노레포의 베이스로 쓸 수 있는 상태"로 다듬기
2. 다른 레포들을 순서대로 흡수하기

## 6.1 Stage A. `orca_auto`를 베이스 레포로 정리

목표:

- 현재 `orca_auto`를 모노레포 루트로 확장 가능한 형태로 만든다

작업:

1. 루트에 `src/` 기반 패키지 구조 도입 준비
2. 현재 `core/` vs `orca_auto/` 이중 구조를 정리
3. `chem_core` alignment가 아직 남은 부분이 있으면 선행 정리
4. docs와 REFERENCE를 "ORCA 앱"과 "미래 모노레포 루트" 관점으로 분리
5. 루트 테스트/CI를 나중에 다중 앱용으로 확장하기 쉬운 형태로 준비

Stage A의 핵심은 "다른 앱을 당장 가져오지 않아도, 레포가 받아들일 준비를 하는 것"이다.

## 6.2 Stage B. 공용 namespace 골격 도입

목표:

- `orca_auto` 레포 안에 `chemstack` namespace를 먼저 만든다

작업:

1. `src/chemstack/__init__.py` 생성
2. `src/chemstack/orca/` 생성
3. 기존 ORCA public CLI를 `chemstack.orca`로 재연결
4. 기존 `orca_auto` import/entrypoint는 임시 호환 계층으로 유지

이 단계의 중요한 점:

- 아직 `chem_core`, `chem_flow`, `xtb_auto`, `crest_auto`를 옮기지 않아도 됨
- 하지만 앞으로 그들이 들어올 패키지 공간을 먼저 만든다

## 6.3 Stage C. `chem_core` 흡수

첫 흡수 대상은 `chem_core`가 가장 적합하다.

이유:

- 다른 대부분의 앱이 이미 의존
- 공용 인프라 계층의 기준이 됨
- `chemstack.core`를 만들지 않으면 나머지 흡수가 애매해짐

작업:

1. `chem_core/chem_core/*` -> `src/chemstack/core/*`
2. `chem_core/tests/*` -> `tests/core/*`
3. import를 `chemstack.core...`로 전환
4. 필요한 임시 alias만 최소로 유지

완료 기준:

- `chemstack.core` 기준으로 core 테스트 통과
- `orca_auto`와 앞으로 들어올 앱이 공용 인프라를 참조할 수 있음

## 6.4 Stage D. `xtb_auto`, `crest_auto` 흡수

이 둘은 구조가 비슷하므로 같은 묶음으로 처리하는 편이 좋다.

작업:

1. `xtb_auto/xtb_auto/*` -> `src/chemstack/xtb/*`
2. `crest_auto/crest_auto/*` -> `src/chemstack/crest/*`
3. tests를 각각 `tests/xtb`, `tests/crest`로 이동
4. `chem_core` import를 `chemstack.core`로 치환
5. CLI를 기존 이름으로 재연결

이 단계가 먼저인 이유:

- 이미 `chem_core` 중심으로 잘 맞춰져 있음
- ORCA보다는 이동 리스크가 낮음
- 모노레포 구조 검증에 좋은 중간 단계가 됨

## 6.5 Stage E. `chem_flow` 흡수

`chem_flow`는 `chemstack.flow`로 이동한다.

작업:

1. `chem_flow/chem_flow/*` -> `src/chemstack/flow/*`
2. `chem_flow/tests/*` -> `tests/flow/*`
3. `tests/cross_repo/*`는 `tests/integration/*`로 재배치
4. sibling repo 가정을 제거
5. repo-root/PYTHONPATH 보정 로직을 모노레포 기준으로 단순화

특히 손볼 가능성이 큰 영역:

- submitter 실행 경로
- sibling config 탐색
- ORCA runtime import fallback
- cross-repo smoke harness

완료 기준:

- `chem_flow`가 더 이상 sibling repo 전제를 갖지 않음
- integration 테스트가 in-repo 기준으로 동작

## 6.6 Stage F. ORCA 최종 정리

이 단계에서 ORCA 코드를 최종적으로 `chemstack.orca` 중심으로 정착시킨다.

작업:

1. `core/`의 ORCA 전용 모듈을 `chemstack.orca` 아래로 재배치
2. 진짜 공용으로 승격할 것만 `chemstack.core`로 이동
3. `from core...` import 제거
4. `orca_auto` facade/shim 정리

완료 기준:

- ORCA 코드가 더 이상 legacy `core/` 구조에 묶이지 않음
- 모노레포 내부 구조가 `chemstack.*` 기준으로 일관됨

## 7. 왜 이 순서가 좋은가

겉보기에는 "다들 `orca_auto`를 참고했으니 ORCA부터 다 합치자"가 자연스러워 보일 수 있다.

하지만 실제로는 아래 순서가 더 안전하다.

- 베이스 레포는 `orca_auto`
- 첫 흡수 대상은 `chem_core`
- 그 다음 `xtb_auto`, `crest_auto`
- 그 다음 `chem_flow`
- ORCA 내부 구조 최종 정리는 마지막

이유:

- `orca_auto`는 가장 중요하고 가장 민감한 코드이므로 초반에 크게 흔들면 안 됨
- `chem_core`를 먼저 넣어야 공용 namespace가 생김
- `xtb_auto`, `crest_auto`는 상대적으로 흡수 리스크가 낮음
- `chem_flow`는 sibling assumptions를 걷어내는 작업이 필요함
- ORCA는 마지막에 정리해도 이미 레포 기준 베이스 역할을 수행할 수 있음

즉, "베이스 레포 선택"과 "가장 먼저 구조를 갈아엎을 대상"은 같지 않다.

## 8. 구체적인 Phase 계획

## Phase 0. 의사결정 확정

확정할 항목:

- repo 베이스는 `orca_auto`
- namespace는 `chemstack`
- CLI는 초기에는 기존 이름 유지
- `src` layout으로 전환
- tests는 루트 `tests/` 계층으로 재배치

산출물:

- 이 계획서 승인
- 최종 디렉터리 구조 합의

## Phase 1. 레포 골격 확장

작업:

1. 루트 `src/chemstack/` 생성
2. 루트 `tests/` 계층 생성
3. 루트 `pyproject.toml`을 장기적으로 단일 패키지 기준으로 재구성할 준비
4. 문서에 모노레포 전환 방향 명시

산출물:

- `chemstack` namespace 골격
- 다중 앱 수용 가능한 레포 틀

## Phase 2. `chem_core` 흡수

작업:

1. 공용 core 코드 이동
2. tests 이동
3. import 전환
4. core-only validation 수행

산출물:

- `chemstack.core`

## Phase 3. xTB/CREST 흡수

작업:

1. 두 앱 코드 이동
2. CLI 유지
3. tests 이동
4. core namespace cutover

산출물:

- `chemstack.xtb`
- `chemstack.crest`

## Phase 4. workflow 흡수

작업:

1. `chem_flow` 이동
2. cross-repo harness를 integration suite로 전환
3. repo-root assumptions 제거

산출물:

- `chemstack.flow`
- `tests/integration`

## Phase 5. ORCA 최종 namespace 정리

작업:

1. ORCA legacy structure 축소
2. `chemstack.orca` 중심 구조 확정
3. compatibility shim 축소

산출물:

- `chemstack.orca`
- legacy `core/` 의존 제거 또는 최소화

## Phase 6. 문서/배포 자산 통합

작업:

1. README 통합
2. config 경로 문서 통합
3. deploy/systemd/scripts/examples 재배치

산출물:

- 모노레포 운영 문서 세트

## 9. CLI/사용자 경험 정책

초기 마이그레이션에서는 사용자 습관을 깨지 않는 것이 중요하다.

권장 정책:

- `orca_auto` 명령 유지
- `xtb_auto` 명령 유지
- `crest_auto` 명령 유지
- `chem_flow` 명령 유지

즉, 내부는 단일 패키지지만 외부 UX는 한동안 그대로 둔다.

필요하면 나중에 추가 alias를 제공할 수 있다.

예:

- `chemstack orca ...`
- `chemstack xtb ...`
- `chemstack flow ...`

하지만 이것은 1차 마이그레이션의 목표가 아니다.

## 10. 테스트 전략

최종 테스트 계층:

- `tests/core`
- `tests/orca`
- `tests/xtb`
- `tests/crest`
- `tests/flow`
- `tests/integration`

권장 CI 순서:

1. `ruff`
2. `mypy`
3. `pytest tests/core`
4. `pytest tests/xtb`
5. `pytest tests/crest`
6. `pytest tests/flow`
7. `pytest tests/orca`
8. `pytest tests/integration`

포인트:

- ORCA는 가장 중요한 축이지만, 초반 마이그레이션에서는 오히려 나중에 돌리는 것이 좋다
- integration은 가장 마지막에 둔다

## 11. 주요 리스크

## 11.1 `orca_auto/core`와 `chemstack.core`의 이름 충돌

리스크:

- 개발자가 "둘 다 core"라고 인식하며 경계를 헷갈릴 수 있음

대응:

- `orca_auto/core`는 legacy ORCA implementation layer로 명확히 규정
- 공용 코어는 `chemstack.core`만으로 정의

## 11.2 ORCA를 너무 일찍 건드리는 것

리스크:

- 가장 안정적인 앱이 가장 먼저 흔들릴 수 있음

대응:

- ORCA는 베이스 레포로 쓰되, 대규모 코드 이동은 후순위로 둔다

## 11.3 `chem_flow`의 sibling repo 전제

리스크:

- submitter, activity, adapters가 repo-root 가정에 민감함

대응:

- `tests/integration` 전환과 함께 단계적으로 제거
- repo-root fallback보다 package-local invocation으로 이동

## 11.4 문서와 실제 경로 불일치

리스크:

- 모노레포 전환 후에도 README가 옛 설치 순서를 안내할 수 있음

대응:

- 문서 통합을 마지막으로 미루지 말고 Phase 1부터 방향을 명시

## 12. Non-goals

이번 1차 계획에서 바로 하지 않는 것:

- CLI 이름 전면 교체
- ORCA artifact contract 변경
- retry ladder semantics 변경
- chemistry engine behavior 재설계
- workflow catalog 재설계

이번 계획의 본질은 "구조 통합"이지 "제품 동작 재정의"가 아니다.

## 13. 최종 권고

정리하면, 네 제안은 매우 좋은 방향이다.

가장 정확한 표현은 이렇다.

- `orca_auto`를 모노레포의 베이스 레포로 삼는다
- 하지만 최종 단일 패키지 이름은 `orca_auto`가 아니라 `chemstack`으로 둔다
- 다른 앱들은 `orca_auto`에 종속되는 것이 아니라, 같은 namespace 아래 sibling으로 들어온다

한 줄 권고:

`repo는 orca_auto를 기반으로 흡수하고, package는 chemstack으로 재편하자.`

## 14. 즉시 다음 액션

이 계획서를 기준으로 바로 다음에 할 수 있는 가장 좋은 작업은 두 가지다.

1. `Phase 0` 결정을 문서상 확정하고
2. `Phase 1`에 해당하는 `src/chemstack/` 골격과 루트 테스트 계층 초안을 실제로 만드는 것

원하면 다음 작업으로 바로 `orca_auto` 안에 `chemstack` 골격을 만드는 초안까지 진행할 수 있다.
