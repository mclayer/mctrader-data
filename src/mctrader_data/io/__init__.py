"""mctrader-data io sub-domain — cold L2/L3 read API + cache + endpoint router.

Relocated from mctrader-engine (MCT-183, ADR-031 §D2 io-relocate). Layer 2 DATA-STORAGE
영역 단독 소유. Dead-in-data — production wiring = MCT-185 cold-read cutover owner.

Public API (Story MCT-154 Phase 2):
- ColdReader  — NAS endpoint cold L2/L3 read API + S6 cross-check (legacy node=DEFAULT mapping)
- ReaderCache — LRU + TTL cache (ADR-027 D9) + explicit flush API + verify barrier (S3)
- EndpointRouter — env 기반 endpoint resolution + atomic flip (immutable swap) + 7d grace mode

Public API (Story MCT-170 Phase 2 — tier promotion reader):
- TierReader  — facade orchestration (priority chain: cache → NAS L1/L2/L3 → local fallback)
- L1Reader    — L1 tier specialized read (prefix tier=L1/, ETag verify)
- DRMode      — DR mode state machine (CLOSED/OPEN/HALF_OPEN/UNKNOWN_TIER) + Prometheus

ADR-027 D4 step 3 (reader endpoint 전환) + D9 (read-through cache) 직접 owner.
ADR-029 D1=C + D4=B + D5=C + D7=A + D8=C (MCT-170 Phase 2 박제).
"""

from mctrader_data.io.cold_reader import ColdReader, ReadResult
from mctrader_data.io.dr_mode import DRMode
from mctrader_data.io.endpoint_router import (
    EndpointFlipResult,
    EndpointRouter,
    GraceModeState,
)
from mctrader_data.io.l1_reader import L1ReadResult, L1Reader
from mctrader_data.io.reader_cache import CacheEntry, CacheFlushResult, ReaderCache
from mctrader_data.io.tier_reader import TierReadResult, TierReader

__all__ = [
    "CacheEntry",
    "CacheFlushResult",
    "ColdReader",
    "DRMode",
    "EndpointFlipResult",
    "EndpointRouter",
    "GraceModeState",
    "L1ReadResult",
    "L1Reader",
    "ReadResult",
    "ReaderCache",
    "TierReadResult",
    "TierReader",
]
