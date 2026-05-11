# mctrader-data

OHLCV + tick + orderbook collector with Parquet/DuckDB storage — ADR-009 v1 schema reference impl. Forward-only WebSocket collector daemon (MCT-58) + backfill (MCT-15).

## Status

`v0.9.0` — Docker-first deployment (CFP-128 / ADR-033). systemd-based deploy removed (BREAKING).

## Public API

```python
from mctrader_data import scan_candles, OhlcvSchema, BackfillRunner
```

## CLI

```bash
mctrader-data backfill --exchange bithumb --symbol KRW-BTC --tf 1h --days 7
mctrader-data backfill --exchange bithumb --symbol KRW-BTC --tf 1h --start 2026-04-25T00:00:00Z --end 2026-05-02T00:00:00Z --policy halt
mctrader-data backfill --exchange bithumb --symbol KRW-BTC --tf 1h --days 7 --dry-run
```

### Legacy candle retirement (ADR-026 / MCT-146)

Epic MCT-112 Story-12 land 후 `backfill` 명령은 **cutoff timestamp** 이후 구간을 자동 거부.
cutoff 이후는 transaction WAL → Compactor → Parquet (Aggregation Core Lib) 가 SSOT.

- **Default cutoff**: `2026-06-01T00:00:00Z` (ADR-026 §D2 placeholder, deployment runbook 이 활성 박제).
- **Override**: 환경변수 `MCTRADER_CUTOFF_TIMESTAMP` (ISO-8601 UTC, month boundary 강제).
- **Escape hatch**: `--allow-post-cutoff` (DR / debug 한정 — operator explicit opt-in).
- **Row-level provenance**: `mctrader_data.provenance.assign_provenance(ts)` →
  `"legacy_candle"` (pre-cutoff, immutable SSOT) / `"transaction_derived"` (post-cutoff).

## Storage layout (ADR-009 D2)

```
{root}/market/ohlcv/schema_version=ohlcv.v1/exchange={ex}/symbol={sym}/timeframe={tf}/year={Y}/month={M}/date={D}/*.parquet
{root}/market/ticks/schema_version=tick.v1/exchange={ex}/symbol={sym}/date={D}/part-{snapshot_id}.parquet
{root}/market/orderbook/schema_version=orderbook.v1/exchange={ex}/symbol={sym}/date={D}/part-{snapshot_id}.parquet
```

Root resolution priority: `--root` > `MCTRADER_DATA_ROOT` > repo-local `data/parquet/`.

## Docker deployment

`v0.9.0` — systemd unit removed. Docker-first deployment (CFP-128 / ADR-033).

### Prerequisites

- Docker Desktop (Windows dev) or Docker Engine 24+ (Linux prod)
- Docker Compose v2 (Compose plugin, included in Docker Desktop)
- Outbound HTTPS + WebSocket access to `pubwss.bithumb.com` and `api.bithumb.com`

### Quick start

```bash
git clone https://github.com/mclayer/mctrader-data.git
cd mctrader-data
docker compose up -d collector
docker compose ps           # STATUS = Up X seconds (healthy) after ~60s
docker compose logs -f collector
```

### Cross-platform parity (Windows dev → Linux prod)

- `docker compose build` produces a `linux/amd64` image regardless of host OS
  (Docker Desktop on Windows runs Linux containers in a managed VM).
- For Linux production hosts: clone the repo and run `docker compose build`
  on the prod host. No image push/pull needed in Pilot.

### Configuration

The container reads these env vars (override via compose `environment:`):

| Var | Default | Purpose |
|---|---|---|
| `MCTRADER_DATA_ROOT` | `/var/lib/mctrader/data` | Parquet output root inside container |
| `MCTRADER_HEALTH_PORT` | `8080` | HealthServer (HTTP `/health`) bind port |
| `MCTRADER_NODE_ID` | (hostname) | HA active-active node identifier (MCT-91) |

To change collector args, override `command:` in compose.yml or use:

```bash
docker compose run --rm collector collect \
  --symbols KRW-BTC,KRW-ETH,KRW-XRP \
  --include transactions,orderbook \
  --log-level INFO
```

### Backfill (one-shot)

```bash
docker compose run --rm collector backfill \
  --exchange bithumb --symbol KRW-BTC --tf 1h --days 7
```

The same image serves both `collect` (daemon) and `backfill` (one-shot) entrypoints.

### Volume DR (data persistence + backup)

Data lives in named volume `mctrader_data` (mounted at `/var/lib/mctrader/data`).

Backup (host-side, ad-hoc):

```bash
docker run --rm \
  -v mctrader_data:/src \
  -v "$(pwd)":/dst \
  alpine tar czf /dst/mctrader_data-$(date +%Y%m%d).tar.gz -C /src .
```

Cron recommendation: 1×/day, 7-day rolling retention. **ADR-009 invariant**: ticks
and orderbook are forward-only (Bithumb public API has no historical replay) →
collector outage = permanent data gap → off-host backup is mandatory.

### Operations

| Action | Command |
|---|---|
| Status | `docker compose ps` |
| Logs (tail) | `docker compose logs -f collector` |
| Healthcheck | `docker compose ps` (STATUS column) |
| Stop (graceful) | `docker compose stop collector` (SIGTERM + 30s drain) |
| Restart | `docker compose restart collector` |
| Disable autostart | `docker compose down` (stops + removes container, volume preserved) |
| Full cleanup (data loss!) | `docker compose down -v` |

### Disaster recovery

- All data forward-only. **No backfill for ticks/orderbook** — Bithumb public API
  does not expose historical tick data. Collector outage = permanent gap for that
  symbol/window.
- For multi-host redundancy, run the collector on 2+ hosts writing to separate
  volumes; reconcile/merge offline. HA active-active partitioning by `node_id`
  (MCT-91 / MCT-93) preserves data integrity across nodes.

### Rollback (Pilot only)

If Pilot validation fails:

```bash
git revert <commit-range>  # restores pre-Docker state
docker compose down -v     # cleanup
```

systemd reinstall is not necessary (production not running on systemd).

## Related

- [mctrader-hub](https://github.com/mclayer/mctrader-hub) — governance / ADR-009
- [mctrader-market](https://github.com/mclayer/mctrader-market) — Candle Protocol contract
- [mctrader-market-bithumb](https://github.com/mclayer/mctrader-market-bithumb) — Bithumb adapter
