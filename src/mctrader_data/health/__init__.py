"""Data accumulation health verification framework — MCT-165.

4-layer MVP: volume / gap / file_count / lag
INV-1: read-only fs walk only (no writes, no corrective actions)
INV-2: start_date default = 2026-05-09 (50-sym cut-in)
INV-3: 4 layer scope frozen (presence/schema/parity follow-up ADR)
INV-4: exit code 0=ALL PASS, 1=any FAIL, 2=tool error
"""
