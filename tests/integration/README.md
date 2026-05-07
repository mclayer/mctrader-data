# mctrader-data Integration Smoke (manual)

> **Why manual?** Bithumb live WebSocket dependency makes CI fragile.
> Run these smokes before/after major Docker / collector changes.
> Cross-platform: same procedure on Windows dev (Docker Desktop) and Linux prod.

## Prerequisites

- Docker Desktop (Windows) or Docker Engine 24+ (Linux)
- Network access to `pubwss.bithumb.com:443` + `api.bithumb.com:443`
- Free disk space ≥ 1 GB for the named volume

## Smoke 1: Build + healthy lifecycle

```bash
cd c:\workspace\mclayer\mctrader-data    # or your clone path
docker compose build
docker compose up -d collector

# PowerShell:
Start-Sleep -Seconds 65
# bash:
sleep 65

docker compose ps
```

**Pass criteria**: `STATUS` column shows `Up X seconds (healthy)`.

If `(unhealthy)` after start_period (60 s) + 1 healthcheck cycle:

```bash
docker compose logs collector --tail=50
docker compose exec collector python -c "import urllib.request; \
  print(urllib.request.urlopen('http://localhost:8080/health').read())"
```

Common failure modes:

- **Bithumb WebSocket reach fail** → check outbound network / DNS.
- **Symbol subscribe fail** → check Bithumb API status; fall back to `--include transactions` only.
- **`SchemaMismatchError` in mctrader-market-bithumb** → known upstream issue. The HealthServer
  will return 503 + `ws_state=disconnected` (Docker correctly marks `unhealthy`). Pilot infra
  is OK; the data ingest path needs a separate fix.

## Smoke 2: Health endpoint self-check (from inside the container)

```bash
docker compose exec collector python -c "import urllib.request, json; \
  r = urllib.request.urlopen('http://localhost:8080/health'); \
  print('status:', r.status); print('body:', r.read().decode())"
```

**Pass criteria**: `status: 200` + JSON body containing `"status":"ok"` and `"ws_state":"connected"`.

If `503` is returned, body explains the reason (`heartbeat unavailable` /
`ws_state=disconnected`). 503 is a *successful response* from the HealthServer
— it means the server is bound and reachable, just that the collector itself
is not in a healthy state.

## Smoke 3: Volume invariant (data preservation across restart)

```bash
docker compose ps                              # collector running, healthy
# Wait 2-5 min so collector accumulates data
docker compose down                            # stop, keep named volume
docker compose up -d collector                 # restart
sleep 65
docker compose exec collector ls /var/lib/mctrader/data/market/
```

**Pass criteria**: `ohlcv/`, `ticks/`, `orderbook/`, `manifest/` directories present
with data from the previous run.

Cleanup if needed: `docker compose down -v` (NOTE: `-v` deletes the named volume.)

## Smoke 4: SIGTERM graceful shutdown (30s drain)

```bash
docker compose up -d collector
sleep 90
time docker compose stop collector             # measure
```

**Pass criteria**: `docker compose stop` returns within 35 s (collector flushes
in-flight writes within the asyncio cancel + finalize path; default Docker
`--time=10` overridden via compose, see compose.yml `stop_grace_period:` if you
need to extend).

If stop hangs > 30 s: SIGKILL fallback. Inspect `docker compose logs collector`
for "graceful shutdown complete" / "heartbeat task shutdown complete" /
"health server shutdown complete" messages. If absent, in-flight writes may be
lost — investigate `collector.py` `_finalize()` path.

## Smoke 5: Backfill (one-shot, optional)

```bash
docker compose run --rm collector backfill \
  --exchange bithumb --symbol KRW-BTC --tf 1h --days 1 --dry-run
```

**Pass criteria**: exit 0 + dry-run output showing planned partition writes.

## State invariants (§8.5 of design spec)

1. **Volume preservation**: Smoke 3 covers — restart preserves `mctrader_data` named volume.
2. **Graceful shutdown**: Smoke 4 covers — SIGTERM → in-flight write flush within 30 s.

Run after every Docker-related change (Dockerfile / compose.yml / heartbeat /
collector.py cleanup logic edits).

## Cutover acceptance evidence (Pilot Story §9)

After all smokes pass, capture evidence into the Story file:

```bash
docker compose up -d collector
sleep 65
docker compose ps > /tmp/cutover-evidence.txt
docker compose exec collector python -c "import urllib.request; \
  r = urllib.request.urlopen('http://localhost:8080/health'); \
  print('STATUS:', r.status); print('BODY:', r.read().decode())" \
  >> /tmp/cutover-evidence.txt
docker compose logs collector --tail=20 >> /tmp/cutover-evidence.txt
docker compose down -v
```

Attach `/tmp/cutover-evidence.txt` content to the Story §9 evidence table.

## Known limitations

- **Bithumb live WebSocket dependency**: Smokes 1-4 require live network access and a
  functioning Bithumb public API. If the upstream API changes its envelope format
  (as observed during MCT-N Pilot — `orderbookdepth missing symbol`,
  `invalid event_time` parsing), `ws_state` will stay `disconnected` and the
  HealthServer will return 503. The Pilot's infra layer (Dockerfile / compose /
  HealthServer) is unaffected by such upstream drift; investigate
  mctrader-market-bithumb's `ws_mapping.normalize_message` for schema patches.
- **Multi-arch image**: Pilot publishes `linux/amd64` only. ARM64 (Raspberry Pi /
  Graviton) requires a follow-on Story to enable buildx multi-platform output.
