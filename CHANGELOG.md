# Changelog

All notable changes to mctrader-data are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
