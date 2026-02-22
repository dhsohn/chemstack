# ORCA Linux 운영 표준화 계획서

## Context

`orca_auto`는 Linux 실행 모델로 정리되었다. 이제 목표는 경로/설정/운영 절차를 단일 기준으로 고정해 장기 운영 안정성을 높이는 것이다.

## 목표

1. 모든 실행 경로를 Linux 절대경로로 고정한다.
2. 설정 스키마를 Linux-only로 유지한다.
3. 런타임/테스트/운영 문서를 동일 기준으로 맞춘다.
4. 신규 기능 추가 시 Linux-only 검증 절차를 기본 게이트로 적용한다.

## 운영 표준

1. 코드 루트: `/home/daehyupsohn/orca_auto`
2. 계산 데이터 루트: `/home/daehyupsohn/orca_runs`
3. ORCA 실행 파일: `/home/daehyupsohn/opt/orca/orca`
4. 기본 설정 파일: `config/orca_auto.yaml`

## 설정 정책

```yaml
runtime:
  allowed_root: "/home/daehyupsohn/orca_runs"
  default_max_retries: 5

paths:
  orca_executable: "/home/daehyupsohn/opt/orca/orca"
```

정책:

1. `allowed_root`, `orca_executable`는 Linux 절대경로만 허용한다.
2. 실행 환경 모드 전환 키는 두지 않는다.
3. 실행 파일 경로는 `.exe` 확장자를 허용하지 않는다.

## 코드 가드레일

1. `core/config.py`
   - Linux 경로 강제
   - 제거된 레거시 설정 키 거부
2. `core/orca_runner.py`
   - Linux ORCA 직접 실행만 허용
3. `core/orchestrator.py`
   - 기본 설정 경로를 저장소/홈 기준으로만 탐색
4. `core/pathing.py`
   - 경로 검증 보조 유틸리티만 유지

## 운영 체크리스트

일일 점검:

1. `scripts/validate_runtime_config.py` 실행
2. `scripts/audit_input_path_literals.py` 실행 (`.inp` 파일 내 Windows 경로 잔존 탐지)
3. `allowed_root` 하위 디렉터리 권한/용량 점검
4. 실패 케이스의 `run_report.json` 원인 분류

배포 전 점검:

1. `PYTHONPATH=. .venv/bin/python -m unittest discover -s tests -v`
2. 샘플 실행 3건
   - 즉시 완료 케이스
   - 재시도 발생 케이스
   - 중단 후 재실행 케이스
3. 문서와 설정 예시가 Linux 경로와 일치하는지 확인

## 성능 관찰 기준

1. `run-inp` 시작 지연 시간
2. 재시도율 (`failed/completed` 비율)
3. `unknown_failure` 비율
4. 디스크 사용량 증가율 (`orca_runs`)

## 리스크와 대응

1. 경로 오입력
   - 대응: 설정 로더에서 즉시 거부
2. ORCA 실행 파일 권한 누락
   - 대응: `validate_runtime_config.py`에서 실행 권한 검증
3. 장기 실행 중단 시 상태 불일치
   - 대응: `run_state.json` 기반 재개 절차 유지
4. 문서 예시와 실제 코드 불일치
   - 대응: 릴리스 전 문서 검수 체크리스트 필수화

## 완료 기준

1. 코드/설정/문서 예시가 Linux-only 경로로 통일됨
2. 단위 테스트/통합 스모크 테스트 통과
3. 신규 변경의 리뷰 체크리스트에 Linux-only 검증 항목이 포함됨
4. 운영자가 단일 실행 절차(`bin/orca_auto`)만 사용함
