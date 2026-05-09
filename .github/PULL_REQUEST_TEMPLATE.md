## Summary

<!-- 변경 사항을 간략히 설명하세요 -->

## Related Issue

<!-- 관련 이슈 또는 Story 번호 (예: MCT-XXX) -->

## Test plan

- [ ] 관련 pytest 통과 확인
- [ ] 로컬 환경에서 직접 검증

## Defensive coding checklist (ADR-018)

- [ ] D1: Decimal/문자열 입력 객체에 `field_validator` 적용 (float/NaN/whitespace/overflow 거부)
- [ ] D2: 도메인 값 객체에 `model_config = ConfigDict(frozen=True)` + 컬렉션은 `tuple[T, ...]`
- [ ] D3: cross-field 불변식이 `@model_validator(mode="after")`로 강제됨
- [ ] D4: check-then-act 카운터/quota가 단일 `threading.Lock` 안에서 원자화됨
- [ ] D5: 지속 파일 쓰기가 `.tmp_{uuid} → fsync → rename` 패턴 사용
- [ ] D6: HTTP header/metadata key 비교가 `.lower()` normalize 후 수행
- [ ] D7: governance decision이 artifact에서 derive되며 CLI flag bypass 불가
- [ ] N/A 표시한 항목은 사유 명시
