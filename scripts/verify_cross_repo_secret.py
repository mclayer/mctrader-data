#!/usr/bin/env python3
"""Verify MCTRADER_CROSS_REPO_TOKEN secret is registered in all 6 mctrader repos.

MCT-177 CO-3 — cross-repo secret 6 repo read 검증.

Usage:
    python scripts/verify_cross_repo_secret.py

Exit codes:
    0 — MCTRADER_CROSS_REPO_TOKEN is present in ALL 6 repos
    1 — one or more repos are missing the secret (repos listed)
    2 — tool error (gh CLI unavailable, auth failure, etc.)
"""

from __future__ import annotations

import subprocess
import sys

ORG = "mclayer"
REPOS = [
    "mctrader-data",
    "mctrader-engine",
    "mctrader-web",
    "mctrader-market",
    "mctrader-signal-collector",
    "mctrader-hub",
]
SECRET_NAME = "MCTRADER_CROSS_REPO_TOKEN"


def _gh_secret_list(repo: str) -> list[str]:
    """Return list of secret names registered in repo via ``gh secret list``."""
    result = subprocess.run(
        ["gh", "secret", "list", "--repo", f"{ORG}/{repo}", "--json", "name", "--jq", ".[].name"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"gh secret list failed for {ORG}/{repo}: {result.stderr.strip()}"
        )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def main() -> int:
    missing: list[str] = []
    errors: list[str] = []

    print(f"Checking secret '{SECRET_NAME}' in {len(REPOS)} repos...")
    for repo in REPOS:
        try:
            secrets = _gh_secret_list(repo)
            if SECRET_NAME in secrets:
                print(f"  [OK]      {ORG}/{repo}")
            else:
                print(f"  [MISSING] {ORG}/{repo}")
                missing.append(repo)
        except RuntimeError as exc:
            print(f"  [ERROR]   {ORG}/{repo}: {exc}")
            errors.append(repo)

    print()
    if errors:
        print(f"[ERROR] Failed to query {len(errors)} repo(s): {errors}")
        print(
            "Ensure 'gh auth login' is complete and the token has 'repo' + 'admin:repo_hook' scope."
        )
        return 2

    if missing:
        print(f"[FAIL] Secret '{SECRET_NAME}' missing in {len(missing)} repo(s):")
        for repo in missing:
            print(f"  - {ORG}/{repo}")
        print()
        print("Registration guide:")
        print(
            "  gh secret set MCTRADER_CROSS_REPO_TOKEN "
            "--repo mclayer/<repo> --body \"$(gh auth token)\""
        )
        print("  (repeat for each missing repo above)")
        return 1

    print(f"[PASS] '{SECRET_NAME}' is present in all {len(REPOS)} repos.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n[INTERRUPTED]")
        sys.exit(2)
