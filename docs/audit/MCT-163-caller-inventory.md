# MCT-163 Caller Inventory — NASUploader.put() Full-repo Scan

Story: MCT-163
Phase: 2.1 preflight
Date: 2026-05-14
Auditor: DeveloperPLAgent

## Scan command

```
grep -rn "\.put(" mctrader-data/src/ --include="*.py"
```

## Actual .put() call sites (실 call site 2건)

| # | File | Line | Context |
|---|------|------|---------|
| 1 | `src/mctrader_data/nas_migration/backfill_orchestrator.py` | 765 | `put_result = self._uploader.put(key=..., data=..., sha256=...)` |
| 2 | `src/mctrader_data/nas_storage/retry_queue.py` | 335 | `result = uploader.put(key=key, data=data, sha256=sha256, suppress_enqueue=True)` |
| 3 | `src/mctrader_data/nas_storage/retry_queue.py` | 386 | `result = uploader.put(key=key, data=data, sha256=sha256, suppress_enqueue=True)` |
| 4 | `src/mctrader_data/nas_storage/dual_writer.py` | 179 | `nas_put_result = self._uploader.put(nas_key, payload, sha256=sha256, ...)` |

## NASUploader instantiation sites (3건)

| # | File | Line | Context |
|---|------|------|---------|
| 1 | `src/mctrader_data/cli.py` | 691 | `nas_uploader = NASUploader(...)` — CLI drain command |
| 2 | `src/mctrader_data/nas_migration/backfill_orchestrator.py` | 352 | `nas_uploader: NASUploader` — parameter injection |
| 3 | `src/mctrader_data/nas_migration/cutover_verifier.py` | 89 | `nas_uploader: NASUploader` — parameter injection |

## Backward compat analysis

- **put(key, data, sha256, suppress_enqueue)** signature 유지 필수 (INV-2)
- 신규 `put_streaming(local_path|fileobj, nas_key, sha256)` method 추가 (D3=A) — 기존 callers 영향 0
- DualWriter.write() 내부만 `put` → `put_streaming` 전환 (F3)
- backfill_orchestrator: `put(data=bytes)` — bytes caller, put_streaming 대상 아님 (cold-path latency tolerable)
- retry_queue: `put(data=bytes, suppress_enqueue=True)` — bytes caller, 영향 0

## R1 risk verdict

- 기존 `put()` signature 보존 → R1 HIGH risk 완화됨
- `put_streaming()` = 신규 method (break 없음)
- AC-2 회귀 test로 2차 검증 예정
