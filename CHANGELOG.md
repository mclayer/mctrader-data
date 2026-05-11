# Changelog

All notable changes to mctrader-data are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [MCT-137] Aggregation Core Lib — 4 bar 알고리즘 + immutable contract metadata

### Added (Epic MCT-112 Story-3, ADR-025)

Pure-Python aggregation core (`mctrader_data.aggregation`) that the Hot
(asyncio) and Cold (DuckDB) paths import directly — SSOT for the 4 information
bar algorithms.

- `TimeBarAggregator(timeframe)` — fixed wall-clock `[start, end)` window bar.
- `VolumeBarAggregator(threshold)` — cumulative base-volume threshold bar.
- `TickBarAggregator(threshold)` — fixed trade-count threshold bar.
- `DollarBarAggregator(threshold)` — cumulative notional (KRW) threshold bar.
- `ContractMetadata` — `frozen=True` dataclass + cached SHA256 contract_id
  (16-hex prefix); version field participates in the hash for ADR-008 SemVer
  rollouts (`info_bar.v1` → `v2` never collides).
- `to_scaled` / `from_scaled` — KRW scaled-integer boundary helpers; reject
  silent precision loss at the entry boundary.

### Contract guarantees

- Tie-breaking SSOT (`tie_breaking="current_tick"`): cumulative metric `==`
  threshold closes the bar **on the triggering tick**.
- Input boundary: `mctrader_market.schemas.tick.TickRowV1_1`.
- Output boundary: `mctrader_market.protocols.information_bar.InformationBarModel`
  (Pydantic v2 frozen).
- Determinism: no random, no threading, no wall-clock reads. 1µs ts_close
  advance applied only when a single-tick bar would violate ADR-009 §D15
  `ts_close > genesis_ts` strict inequality.

## [MCT-106] Zero-loss ingestion pipeline — WAL + tiered compaction

### Breaking behavior change (Phase C, `mctrader-data collect`)

`mctrader-data collect` no longer writes Parquet files synchronously.

**Before:** `collect` wrote Parquet files to `market/<channel>/...` immediately on each batch (500 events).

**After:** `collect` (now `bithumb-ingester`) writes NDJSON WAL files only. Parquet files appear in `market/<channel>/.../tier=L{1,2,3}/...` after the Compactor processes sealed segments (≤ 5 minutes for L1).

**Migration required for callers that read Parquet immediately after collect:**
- Integration tests: use `compact_now()` fixture from `tests/conftest.py`.
- Manual: run `mctrader-data compact --root <path> --once` before reading.

### New commands
- `mctrader-data compact --root <path>` — run the Compactor (L1/L2/L3 tiered)
- `mctrader-data compact --root <path> --once` — single scan cycle and exit

### New services (compose.yml)
- `bithumb-ingester` — replaces `collector`
- `compactor` — new dedicated service

### Deprecated (DeprecationWarning emitted)
- `TickWriter`, `OrderbookWriter`, `OrderbookSnapshotWriter` — replaced by `WalIngester + L1Compactor`

## [0.9.0] — 2026-05-07

### BREAKING
- `deploy/mctrader-collector.service` removed. systemd-based deployment is no longer
  supported. Migrate to Docker (see README §"Docker deployment").
- `deploy/README.md` removed; deployment guide consolidated into the repo README.

### Added
- `Dockerfile` (2-stage, python:3.12-slim, non-root user `mctrader` UID 1001).
- `compose.yml` (collector daemon, named volume `mctrader_data`, healthcheck via
  HTTP `/health` endpoint).
- `.dockerignore` (build context minimization + secret leak prevention; README.md
  exception preserved for hatch).
- `src/mctrader_data/health_server.py` — stdlib `http.server` + daemon thread
  `HealthServer`. `GET /health` → 200/503 based on `HeartbeatWriter._ws_state`.
  Bound to `MCTRADER_HEALTH_PORT` (default 8080), internal-only (compose `ports:`
  not exposed).
- `mctrader-data collect --health-port` CLI option to override default port.
- `MultiSymbolCollector(... health_server=)` constructor argument; lifecycle
  start/stop wired alongside the existing heartbeat task.
- `.claude/_overlay/project.yaml`: `infra_strategy: docker_first` (codeforge ADR-033).
- `.github/workflows/image-lint.yml` (hadolint, failure-threshold=warning).
- `tests/integration/README.md` (5-smoke manual procedure including SIGTERM
  graceful shutdown and named-volume preservation invariants).
- `tests/test_health_server.py` (4 TDD scenarios: missing writer / connected /
  disconnected / port env override).

### Removed
- `deploy/` directory and its contents.

### Changed
- `README.md` deployment section replaced (systemd → Docker), Status line
  bumped to v0.9.0.
- `pyproject.toml` version `0.8.0` → `0.9.0`.

### References
- codeforge ADR-033 / CFP-128 (Docker-first Infra Engineering, Accepted 2026-05-07).
- mctrader Containerization Epic (Pilot Phase 1 — this release).
