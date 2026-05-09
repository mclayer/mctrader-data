"""Bithumb WS spec verification — MCT-104 Phase 2 entry blocker (P2-F-002 + P2-F-004).

Tries 4 modes against wss://pubwss.bithumb.com/pub/ws:
- A: type='orderbooksnapshot' (Researcher 추정 literal)
- B: type='orderbookdepth' + isOnlySnapshot=true (changelog 권장)
- C: type='orderbookdepth' (현재 collector 사용 mode, baseline)
- D: type='orderbooksnapshot' alt casing variants

Captures up to 3 messages per mode, prints raw payload + structural diff.
"""
from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timezone

import websockets

URL = "wss://pubwss.bithumb.com/pub/ws"
SYMBOL = "BTC_KRW"
TIMEOUT_S = 10.0
MAX_MSGS = 3


async def probe(label: str, sub: dict, *, timeout: float = TIMEOUT_S, max_msgs: int = MAX_MSGS) -> dict:
    result = {"label": label, "subscribe": sub, "messages": [], "error": None, "elapsed_s": None}
    start = datetime.now(timezone.utc)
    try:
        async with websockets.connect(URL) as ws:
            # Bithumb sends {"status":"0000","resmsg":"Connected Successfully"} on connect
            try:
                hello = await asyncio.wait_for(ws.recv(), timeout=3.0)
                result["hello"] = json.loads(hello) if hello else None
            except Exception as e:
                result["hello"] = f"ERR: {e}"
            await ws.send(json.dumps(sub))
            # Bithumb may send a subscribe ack
            try:
                ack = await asyncio.wait_for(ws.recv(), timeout=3.0)
                result["ack"] = json.loads(ack) if ack else None
            except Exception as e:
                result["ack"] = f"ERR: {e}"
            count = 0
            while count < max_msgs:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                except asyncio.TimeoutError:
                    result["error"] = f"timeout after {timeout}s waiting for msg #{count + 1}"
                    break
                try:
                    msg = json.loads(raw)
                except Exception:
                    msg = {"_raw": raw[:500]}
                result["messages"].append(msg)
                count += 1
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        result["elapsed_s"] = (datetime.now(timezone.utc) - start).total_seconds()
    return result


async def main() -> None:
    cases = [
        ("A_orderbooksnapshot_literal", {"type": "orderbooksnapshot", "symbols": [SYMBOL]}),
        ("B_orderbookdepth_isOnlySnapshot", {"type": "orderbookdepth", "symbols": [SYMBOL], "isOnlySnapshot": True}),
        ("C_orderbookdepth_baseline", {"type": "orderbookdepth", "symbols": [SYMBOL]}),
        ("D_orderbookSnapshot_camel", {"type": "orderbookSnapshot", "symbols": [SYMBOL]}),
    ]
    for label, sub in cases:
        print("=" * 80)
        print(f"PROBE {label}")
        print(f"  subscribe: {json.dumps(sub)}")
        r = await probe(label, sub)
        print(f"  hello: {json.dumps(r.get('hello'), ensure_ascii=False)[:200]}")
        print(f"  ack:   {json.dumps(r.get('ack'), ensure_ascii=False)[:300]}")
        print(f"  err:   {r.get('error')}")
        print(f"  elapsed: {r.get('elapsed_s'):.2f}s")
        for i, m in enumerate(r["messages"]):
            print(f"  msg[{i}] keys: {list(m.keys())}")
            print(f"  msg[{i}] full: {json.dumps(m, ensure_ascii=False)[:1500]}")
        sys.stdout.flush()


if __name__ == "__main__":
    asyncio.run(main())
