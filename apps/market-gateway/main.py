import os
import json
import asyncio
from typing import Dict, Set, Tuple
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from aiokafka import AIOKafkaConsumer
import uvloop

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("KLINES_TOPIC", "klines_raw")

app = FastAPI(title="Market Gateway (WS)")

# Hub: (symbol, timeframe) -> set of subscriber queues
Subs: Dict[Tuple[str, str], Set[asyncio.Queue]] = {}
_started = False

def key(sym: str, tf: str):  # normalizer
    return (sym.upper(), tf.lower())

async def kafka_loop():
    """
    One consumer reading Kafka and fanning out messages to in-memory queues.
    Message format in Kafka (from market-ingestor):
      { symbol: "BTCUSDT", timeframe: "1m", kline: { t,o,h,l,c,x,... } }
    We normalize to:
      { symbol, tf, t, o, h, l, c, closed }
    """
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        enable_auto_commit=True,
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    await consumer.start()
    print("WS-Gateway: consuming", TOPIC)
    try:
        async for msg in consumer:
            v = msg.value
            k = v.get("kline", {})
            sym = (v.get("symbol") or k.get("s") or "").upper()
            tf = (v.get("timeframe") or k.get("i") or "").lower()
            if not sym or not tf:
                continue
            payload = {
                "symbol": sym,
                "tf": tf,
                "t": int(k.get("t")),            # ms since epoch
                "o": float(k.get("o")),
                "h": float(k.get("h")),
                "l": float(k.get("l")),
                "c": float(k.get("c")),
                "closed": bool(k.get("x", False))
            }
            subs = Subs.get(key(sym, tf))
            if subs:
                # non-blocking fanout
                for q in list(subs):
                    if not q.full():
                        q.put_nowait(payload)
    finally:
        await consumer.stop()

@app.on_event("startup")
async def startup():
    global _started
    if not _started:
        _started = True
        loop = asyncio.get_running_loop()
        loop.create_task(kafka_loop())

@app.get("/health")
async def health():
    return {"status": "ok", "subs": {f"{k[0]}:{k[1]}": len(v) for k, v in Subs.items()}}

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket, symbol: str, tf: str):
    """
    Client connects: ws://host:8001/ws?symbol=BTCUSDT&tf=1m
    Server pushes normalized frames:
      {symbol, tf, t, o, h, l, c, closed}
    """
    await ws.accept()
    s_key = key(symbol, tf)
    q: asyncio.Queue = asyncio.Queue(maxsize=1000)
    Subs.setdefault(s_key, set()).add(q)
    try:
        # heartbeat (optional)
        await ws.send_json({"type": "hello", "symbol": s_key[0], "tf": s_key[1]})
        while True:
            # send data if any, small timeout to allow recv pings (optional)
            try:
                msg = await asyncio.wait_for(q.get(), timeout=15.0)
                await ws.send_json(msg)
            except asyncio.TimeoutError:
                # keep-alive ping
                await ws.send_json({"type": "ping"})
            # (optional) read messages from client to keep connection alive
            if ws.client_state.name != "CONNECTED":
                break
    except WebSocketDisconnect:
        pass
    finally:
        Subs[s_key].discard(q)
        if not Subs[s_key]:
            Subs.pop(s_key, None)
