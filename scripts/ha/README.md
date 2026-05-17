# scripts/ha/ — mctrader-data Active-Active HA Ops

Ops/deployment artifacts for **MCT-89 Epic** (Collector HA Active-Active Multi-Node).

Phase 5 (X5) deliverable. Sister Stories MCT-91 (X2 writer/heartbeat) / MCT-92 (X3 scan/dedup) / MCT-93 (X4 status CLI) are MERGED.

## Files

| File | Purpose |
|---|---|
| `mctrader-data-collector.service` | systemd unit (Restart=always, SIGTERM graceful drain, StartLimitBurst=20/300s) |
| `collector.env.example` | per-host env template (NODE_ID, MCTRADER_DATA_ROOT) |
| `deploy.yml` | Ansible rolling deploy (`serial: 1` + shared-storage deploy lock + heartbeat health gate) |
| `inventory.example.ini` | sample Ansible inventory |
| `heartbeat_health_check.sh` | thin wrapper for `mctrader-data status` (forwards exit code 0/1/2) |

## Host prerequisites

### OS

- Linux distro with **systemd 245+** (Ubuntu 22.04+, RHEL 9+, Debian 12+ all OK)
- `journalctl` for log inspection (`journalctl -u mctrader-data-collector`)

### Python runtime

- **Python 3.11+** (3.13 supported)
- `uv` package manager — `curl -LsSf https://astral.sh/uv/install.sh | sh` (or pinned version per CI)
- Virtualenv at `/opt/mctrader/.venv` with `mctrader-data` installed (`uv pip install -e /opt/mctrader/mctrader-data`)

### Shared storage

- NFS / SMB / Ceph mounted at the **same path on every node** (`/mnt/shared/mctrader/data`)
- **Atomic rename guarantee**:
  - NFSv3 with close-to-open consistency, OR
  - NFSv4, OR
  - Ceph (same pool), OR
  - SMB v3 (same share)
- **Same uid:gid** for the `mctrader` user on every node (POSIX permission semantics depend on this)
- The data root must be writable by `mctrader:mctrader` (heartbeat + parquet + manifest writes)

### Time sync

- NTP/chrony with **clock drift `<1s`** across all nodes
- Heartbeat freshness threshold (default 30s red) requires reasonably synchronized wall-clocks
- Verify: `timedatectl status` should show `System clock synchronized: yes`

### Network

- Outbound HTTPS/WebSocket to `wss://pubwss.bithumb.com/pub/ws` (Bithumb public, no API key)
- SSH key + sudoers entry for the deploy operator (Ansible `become: true`)
- Inter-node connectivity NOT required (active-active uses shared storage, not direct comms)

### Capacity (X7 Calibration backfill, 2026-05-06)

Synthetic measurement basis: 10s two-process simulation, 20 ticks/sec/node + 7-day partition synthetic (70k events / 1.30 MB on disk). Real production figures depend on Bithumb traffic + symbol count and may differ — operators should re-measure post-deployment.

| Resource | Synthetic measurement | Extrapolated (30 min run) | Recommended host provisioning |
|---|---|---|---|
| RAM (asyncio + queue per process) | < 100 MB peak | ~150 MB sustained | **2 GB available per node** |
| Disk growth rate | ~19 KB/event | ~1 GB/day per symbol/tier | **100 GB shared storage** |
| events/sec sustained | 40/sec total (2 nodes × 20/sec) | scales linearly | network-bound (Bithumb peak ~100/sec/symbol) |
| p99 scan latency (7-day partition) | 1.4s | scales linearly with partition size | **< 5s SLA target** at 30-day partition |
| CPU | 2 vCPU per node | I/O-bound, mostly idle | 2 vCPU per node sufficient |

Reproduce these measurements: `python scripts/ha/calibration/c1_dedup_throughput.py` + `python scripts/ha/calibration/c2_scan_latency.py`. Full results: [EPIC-RESULTS-MCT-89.md](https://github.com/mclayer/mctrader-hub/blob/main/docs/retros/EPIC-RESULTS-MCT-89.md) §3 (mctrader-hub governance).

## Initial setup (per node)

```bash
# 1. Create user
sudo useradd -r -m -d /opt/mctrader -s /bin/bash mctrader

# 2. Mount shared storage at the standard path (uid:gid match the mctrader user)
sudo mkdir -p /mnt/shared/mctrader/data
sudo mount -t nfs nas.example.com:/mctrader /mnt/shared/mctrader/data
# Make persistent in /etc/fstab

# 3. Install runtime + clone both repos
curl -LsSf https://astral.sh/uv/install.sh | sudo -u mctrader sh
sudo -u mctrader bash -lc '\
  cd /opt/mctrader && \
  git clone https://github.com/mclayer/mctrader-data.git && \
  git clone https://github.com/mclayer/mctrader-hub.git && \
  uv venv && uv pip install -e ./mctrader-data \
'

# 4. Configure per-host env (uses ops artifacts from mctrader-hub)
sudo mkdir -p /etc/mctrader
sudo cp /opt/mctrader/mctrader-hub/scripts/ha/collector.env.example /etc/mctrader/collector.env
sudo chown root:mctrader /etc/mctrader/collector.env
sudo chmod 0640 /etc/mctrader/collector.env
# Edit /etc/mctrader/collector.env to set NODE_ID per host

# 5. Install systemd unit
sudo cp /opt/mctrader/mctrader-hub/scripts/ha/mctrader-data-collector.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable mctrader-data-collector
sudo systemctl start mctrader-data-collector

# 6. Verify
sudo systemctl status mctrader-data-collector
sudo -u mctrader /opt/mctrader/.venv/bin/mctrader-data status --root /mnt/shared/mctrader/data
```

## Rolling deploy

```bash
# Dry-run first (always)
ansible-playbook -i scripts/ha/inventory.example.ini scripts/ha/deploy.yml --check --diff

# Production
ansible-playbook -i scripts/ha/inventory.example.ini scripts/ha/deploy.yml
```

The playbook:
1. Acquires shared-storage `.deploy_lock` (atomic mkdir on shared storage — single in-flight deploy)
2. For each host (`serial: 1`): peer health gate → stop → git pull → uv sync → start → wait self green
3. Releases lock at end (or stays for manual recovery if interrupted)

### Stale lock recovery

If a deploy is interrupted (operator killed Ansible, network failure, etc.) the `.deploy_lock` directory may persist. Inspect ownership before manually clearing:

```bash
cat /mnt/shared/mctrader/.deploy_lock/owner
# host: node-a.example.com
# time_iso: 2026-05-06T14:23:01Z
# ansible_pid: 12345

# If owner is stale (>1h old, no active Ansible run), remove manually:
sudo rmdir /mnt/shared/mctrader/.deploy_lock
```

### Manual rollback

Per Epic decision #15, **auto-rollback is out of scope**. To revert a bad deploy:

```bash
cd /opt/mctrader/mctrader-data
git revert <bad-commit-sha>
ansible-playbook -i scripts/ha/inventory.example.ini scripts/ha/deploy.yml
```

## Health probe

### Direct CLI

```bash
mctrader-data status --root /mnt/shared/mctrader/data
echo $?   # 0=green, 1=yellow, 2=red/missing-heartbeat
```

### Wrapper (for systemd timers, cron, monitoring)

```bash
/opt/mctrader/mctrader-hub/scripts/ha/heartbeat_health_check.sh
```

The script `exec`s `mctrader-data status` directly so the parent's exit code is preserved.

### JSON for downstream parsing (X6 Streamlit panel)

```bash
mctrader-data status --root /mnt/shared/mctrader/data --format json --no-color
```

## Acceptance verification (X5 done bar)

The following commands must pass in CI (or local equivalent):

```bash
# Ansible playbook syntax + dry-run
ansible-playbook -i scripts/ha/inventory.example.ini scripts/ha/deploy.yml --check --diff --syntax-check

# Ansible best-practices lint
ansible-lint scripts/ha/deploy.yml

# Bash shellcheck
shellcheck scripts/ha/heartbeat_health_check.sh

# systemd unit syntax
systemd-analyze verify scripts/ha/mctrader-data-collector.service
```

Real 2-node E2E demo is **deferred to Phase 7 (X7) Calibration**. X5 freezes artifacts only.

## Out-of-scope (X6/X7/v2)

- Streamlit `00_status` web panel — **X6 (mctrader-web)**, consumes `mctrader-data status --format json`
- 양 node 30분 E2E demo + RAM/disk Calibration C1/C2 — **X7 (mctrader-hub Epic close)**
- Slack/email proactive alerting — **v2 후속** (decision #14)
- k8s/Nomad orchestration — **명시적 거부** (decision #3)
- Auto-rollback on health gate failure — **거부** (decision #15, manual `git revert`)
