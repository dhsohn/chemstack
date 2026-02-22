# ORCA 레거시 제거 완료 보고서

## Context

저장소와 실행 환경은 단일 Linux 기준으로 정리되었다. 본 문서는 제거 범위와 유지해야 할 운영 가드레일을 기록한다.

## 제거 완료 범위

코드:

1. 실행 모드 분기 제거
2. 이종 플랫폼 전용 경로 변환 로직 제거
3. 런타임 종료 보조 분기 제거
4. 설정 파일의 모드 키 제거

스크립트:

1. 레거시 래퍼 스크립트 제거
2. 이관 전용 변환 스크립트 제거

문서:

1. 레거시 실행 예시 제거
2. 단일 Linux 경로 예시로 통일

테스트:

1. 레거시 분기 테스트 제거
2. Linux-only 검증 테스트로 재편

## 현재 운영 기준

1. 실행 명령: `./bin/orca_auto`
2. 설정 파일: `config/orca_auto.yaml`
3. 데이터 루트: `/home/daehyupsohn/orca_runs`
4. ORCA 실행 파일: `/home/daehyupsohn/opt/orca/orca`

## 영구 가드레일

1. 제거된 레거시 설정 키 입력 시 시작 즉시 오류
2. 경로 필드는 Linux 절대경로만 허용
3. `.exe` 실행 파일 경로 금지
4. 문서/테스트 예시는 Linux 경로만 허용

## 검증 절차

런타임 점검:

```bash
PYTHONPATH=. .venv/bin/python scripts/validate_runtime_config.py --config config/orca_auto.yaml
```

테스트 점검:

```bash
PYTHONPATH=. .venv/bin/python -m unittest discover -s tests -v
```

## 운영 리스크와 대응

1. 경로 오입력
   - 대응: 설정 로더 검증 + 운영 체크리스트
2. 실행 파일 교체 누락
   - 대응: 실행 권한/경로 검증 스크립트 주기 실행
3. 문서 드리프트
   - 대응: 릴리스 전 문서 lint와 예시 명령 검수

## 완료 기준

1. 코드/스크립트/문서에 레거시 실행 흔적 없음
2. 테스트 전수 통과
3. 운영자가 단일 실행 절차만 사용
4. 신규 변경 PR에 Linux-only 체크 항목 포함
