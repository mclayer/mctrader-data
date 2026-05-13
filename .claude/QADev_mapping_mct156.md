# QADev 매핑표 — MCT-156 Phase 2

**작성**: QADeveloperAgent  
**Story**: MCT-156 (Phase 2 — compactor NAS wiring + 7종 invariant + Stage 3 ADR-027 amendment)  
**Test File**: `tests/integration/test_compactor_nas_wiring.py`  
**Status**: ✅ 8개 테스트 파일 작성 완료 (구현은 DeveloperPL 병렬 진행)

## §8 Test Contract ↔ 테스트 함수 매핑

| §8 항목 # | 테스트 함수 | 파일 | 커버리지 유형 | 검증 invariant |
|----------|-----------|------|--------------|----------------|
| **Test 1** | `test_l2_committed_partition_appears` | `tests/integration/test_compactor_nas_wiring.py:106` | 정상 경로 (L2 committed) | DualWriter status="committed" → local rename + nas_key 출현 |
| **Test 2** | `test_l3_committed_partition_appears` | `tests/integration/test_compactor_nas_wiring.py:151` | 정상 경로 (L3 committed) | DualWriter status="committed" → L3 partition (no hour) |
| **Test 3** | `test_nas_unreachable_local_only_enqueue` | `tests/integration/test_compactor_nas_wiring.py:192` | 엣지 케이스 (NAS unreachable) | NASUploader.put() status="queued" → DualWriteResult.status="local_only" + local rename |
| **Test 4** | `test_retry_hard_floor_sop_escalation` | `tests/integration/test_compactor_nas_wiring.py:231` | 엣지 케이스 (hard_floor) | NASUploader.put() status="hard_floor_blocked" → DualWriteResult.status="hard_floor_blocked" + tmp rollback |
| **Test 5** | `test_l1_no_nas_upload` | `tests/integration/test_compactor_nas_wiring.py:272` | S3 invariant (ADR-027 D5) | L1 compaction grep: dual_writer/DualWriter 호출 absence |
| **Test 6** | `test_legacy_minio_uploader_no_callsite` | `tests/integration/test_compactor_nas_wiring.py:310` | deprecation verify | grep MinioUploader() in cli.py + runner.py = 0 |
| **Test 7** | `test_prometheus_dual_write_counter_emit` | `tests/integration/test_compactor_nas_wiring.py:337` | Prometheus metric | dual_write_result_total Counter label {status, tier} 존재 + enum verify |
| **NFR-1** | `test_l2_compaction_latency_baseline` | `tests/integration/test_compactor_nas_wiring.py:375` | perf baseline (MCT-148) | L2 latency < 3000ms (p99 50MB=2870.65ms baseline) |

## Fixture 재사용 (MCT-150/151 정합)

| Fixture | 출처 | 용도 |
|---------|------|------|
| `tmp_nas_bucket` | 신규 (local fs mock) | S3 bucket simulation |
| `retry_queue` | MCT-150 pattern | RetryQueue persistent WAL |
| `mock_nas_uploader` | MCT-150 pattern | NASUploader(endpoint, access_key, secret_key, bucket, retry_queue) |
| `dual_writer` | 신규 (DualWriter inject) | DualWriter(nas_uploader, local_root, metrics=None) |
| `sample_ohlcv_payload` | 신규 (minimal Parquet) | Payload bytes for DualWriter.write() |
| `sample_ohlcv_sha256` | 신규 (hashlib) | sha256 hex for verification |

## Test Design 원칙 (TDD 정합)

- **RED**: 모든 테스트는 구현 이전에 **실패하도록 설계**
  - Test 1~4: mock NASUploader.put() return value에 의존 → DualWriter 구현 검증
  - Test 5~6: grep 기반 정적 검증 → runner.py / cli.py 변경 검증
  - Test 7: Prometheus Counter 존재 여부 검증 → prometheus_exporters.py 정합
  - Test 8: latency 측정 → 성능 baseline 검증

- **Assertions**: 각 테스트는 1개의 핵심 불변식만 검증
  - Test 1: `result.status == "committed"` + `nas_key 호출`
  - Test 2: `result.status == "committed"` (L3 no hour partition)
  - Test 3: `result.status == "local_only"` + `local_path exists`
  - Test 4: `result.status == "hard_floor_blocked"` + `tmp_dw not exists`
  - Test 5: `"dual_writer" not in l1_method` (grep)
  - Test 6: `MinioUploader() count == 0` (grep)
  - Test 7: `dual_write_result_total exists` + `labels {status, tier}`
  - Test 8: `latency_ms < 3000.0` (NFR-1)

## Mock 전략 (격리 + 결정성)

| Mock | 방식 | 이유 |
|------|------|------|
| `NASUploader.put()` | return value override | NAS endpoint 실제 호출 제거 (결정성) |
| `dual_writer.write()` | MagicMock 추적 | L1 no-call 검증 |
| `Prometheus Counter.labels()` | attribute 검증 | metric 정의 존재 여부 |
| `sample payload` | minimal Parquet bytes | 실제 payload size 시뮬레이션 (NFR-1 NAS latency) |

## 실행 환경 요구

- Python >= 3.11 (pyproject.toml 정합)
- pytest (MCT-150 CI 정합)
- pytest-benchmark (선택, latency test 개선용)
- mctrader-data[dev] installed editable mode

## CI 통합

Phase 2 별 PR 의 GitHub Actions workflow:

```yaml
- name: Run compactor NAS wiring tests
  run: |
    pytest tests/integration/test_compactor_nas_wiring.py -v --tb=short
  env:
    PYTHONPATH: src:tests
```

## 공백 / 질의 (ArchitectPLAgent 확인 필요)

### 없음 ✅

모든 §8 항목이 테스트 함수로 매핑됨.
§8 Test Contract 명시도 명확함.

## invariant 커버

| invariant | 테스트 | 박제 위치 |
|-----------|--------|----------|
| **committed status** | Test 1, 2 | DualWriter.write() phase 2 commit path |
| **local_only status** | Test 3 | NASUploader.put() status="queued" → propagation |
| **hard_floor_blocked status** | Test 4 | NASUploader.put() status="hard_floor_blocked" → tmp rollback |
| **L1 S3 invariant** | Test 5 | ADR-027 D5: L1 hot path no NAS upload |
| **MinioUploader deprecation** | Test 6 | call site grep=0 (legacy removal) |
| **Prometheus emit** | Test 7 | dual_write_result_total{status, tier} Counter |
| **Latency baseline** | Test 8 | L2 latency < 3000ms (NFR-1 MCT-148) |

## 발견 사항 (production 코드 읽기 전용)

### ✅ 없음

모든 src/ 코드는 이미 MCT-150/151에서 구현완료.
Phase 2는 CompactorRunner integration 및 Prometheus emit 추가만 필요 (DeveloperPL 담당).

## 다음 단계

1. **구현 레인**: DeveloperPL이 CompactorRunner._run_l2() / _run_l3() 에 DualWriter inject
2. **통합**: src/mctrader_data/compactor/runner.py 에서 dual_writer.write() 호출
3. **CI 실행**: Phase 2 PR CI 에서 `pytest tests/integration/test_compactor_nas_wiring.py` 자동 실행
4. **검증**: 모든 8개 테스트 PASS 후 ArchitectPLAgent 감사 (Phase 2 Quality Gate AC-G)
