import os, asyncio, json, orjson, signal
from typing import Dict, Set, Tuple
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from aiokafka import AIOKafkaConsumer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_RAW    = os.getenv("TOPIC_RAW", "klines_raw")   # từ market-ingestor
GROUP_ID     = os.getenv("GROUP_ID", "realtime-api-1")

app = FastAPI(title="Realtime API (WS from Kafka)")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# key = (symbol_upper, timeframe)  => set(WebSocket)
subscribers: Dict[Tuple[str,str], Set[WebSocket]] = {}
sub_lock = asyncio.Lock()

shutdown_event = asyncio.Event()

def _norm_pair(symbol: str, tf: str):
    return (symbol.strip().upper(), tf.strip().lower())

async def register(ws: WebSocket, symbol: str, tf: str):
    key = _norm_pair(symbol, tf)
    async with sub_lock:
        subscribers.setdefault(key, set()).add(ws)

async def unregister(ws: WebSocket, symbol: str, tf: str):
    key = _norm_pair(symbol, tf)
    async with sub_lock:
        if key in subscribers:
            subscribers[key].discard(ws)
            if not subscribers[key]:
                subscribers.pop(key, None)

async def broadcast(symbol: str, tf: str, payload: str):
    key = _norm_pair(symbol, tf)
    targets = None
    async with sub_lock:
        targets = list(subscribers.get(key, []))
    # gửi không chặn – loại client hỏng
    dead = []
    for ws in targets:
        try:
            await ws.send_text(payload)
        except Exception:
            dead.append(ws)
    if dead:
        async with sub_lock:
            s = subscribers.get(key, set())
            for ws in dead:
                s.discard(ws)
            if not s:
                subscribers.pop(key, None)

@app.get("/health")
def health():
    return {"status": "ok", "subs": {f"{k[0]}-{k[1]}": len(v) for k,v in subscribers.items()}}

@app.websocket("/ws")
async def ws_stream(ws: WebSocket,
                    symbol: str = Query(...),
                    tf: str = Query(...)):
    await ws.accept()
    await register(ws, symbol, tf)
    try:
        # ping keepalive
        while True:
            # nếu client gửi gì đó thì bỏ qua – đây là stream 1 chiều
            try:
                _ = await asyncio.wait_for(ws.receive_text(), timeout=20.0)
            except asyncio.TimeoutError:
                await ws.send_text('{"type":"ping"}')
            except WebSocketDisconnect:
                break
    finally:
        await unregister(ws, symbol, tf)

async def kafka_loop():
    """
    Consumer Kafka -> lọc theo (symbol, timeframe) -> broadcast.
    Message nguồn (ví dụ từ Binance raw) như bạn đã thấy:
      {"symbol":"BTCUSDT","timeframe":"1m","kline":{...,"t":..., "o":"...", "h":"...", "l":"...", "c":"...", "x":false}, "ts_event":...}
    Ta chuẩn hóa lại cho FE:
      {"symbol":"BTCUSDT","tf":"1m","time":<sec>,"open":..,"high":..,"low":..,"close":..,"closed":<bool>}
    """
    consumer = AIOKafkaConsumer(
        TOPIC_RAW,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        enable_auto_commit=True,
        value_deserializer=lambda v: v,  # nhận bytes, tự parse để tiết kiệm
        auto_offset_reset="latest",
        max_poll_records=200,
    )
    await consumer.start()
    try:
        while not shutdown_event.is_set():
            batch = await consumer.getmany(timeout_ms=1000)
            for tp, records in batch.items():
                for rec in records:
                    try:
                        msg = json.loads(rec.value)
                        sym  = msg.get("symbol") or (msg.get("k") or {}).get("s")
                        tf   = msg.get("timeframe") or (msg.get("k") or {}).get("i")
                        k    = msg.get("kline") or msg.get("k") or {}
                        if not sym or not tf or not k:
                            continue
                        out = {
                            "symbol": sym.upper(),
                            "tf": str(tf),
                            "time": int((k.get("t") or 0) // 1000),
                            "open": float(k.get("o") or 0),
                            "high": float(k.get("h") or 0),
                            "low":  float(k.get("l") or 0),
                            "close":float(k.get("c") or 0),
                            "closed": bool(k.get("x", False)),
                        }
                        payload = orjson.dumps(out).decode()
                        await broadcast(out["symbol"], out["tf"], payload)
                    except Exception:
                        # nuốt lỗi 1 record; real world: log.warn
                        continue
    finally:
        await consumer.stop()

@app.on_event("startup")
async def on_start():
    loop = asyncio.get_event_loop()
    loop.create_task(kafka_loop())
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: shutdown_event.set())

@app.on_event("shutdown")
async def on_shutdown():
    shutdown_event.set()
