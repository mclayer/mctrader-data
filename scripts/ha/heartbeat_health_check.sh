#!/usr/bin/env bash
# heartbeat_health_check.sh — thin wrapper for `mctrader-data status`
#
# Codex F-3 fix (Phase 5 design review): no jq dependency, no JSON parsing.
# The CLI itself owns health classification + threshold logic; this script
# just forwards the exit code (0=green, 1=yellow, 2=red/missing-heartbeat).
#
# Usage:
#   heartbeat_health_check.sh                   # uses MCTRADER_DATA_ROOT env
#   heartbeat_health_check.sh --no-color        # for piped/CI output
#   heartbeat_health_check.sh --format json     # JSON for downstream parsers
#
# Refs:
#   docs/domain-knowledge/contracts/heartbeat-schema.v1.md (freshness/lag thresholds)
#   docs/stories/MCT-93.md §5.1 (status CLI exit code contract)
set -u

ROOT="${MCTRADER_DATA_ROOT:-/mnt/shared/mctrader/data}"

# Pass through any extra flags (--no-color, --format json, threshold overrides).
exec mctrader-data status --root "$ROOT" "$@"
