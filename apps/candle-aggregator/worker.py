import os
import json
import asyncio
import asyncpg
from aiokafka import AIOKafkaConsumer
import uvloop

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/market")

async def upsert_candle(conn, symbol, timeframe, ts_ms, o, h, l, c, v):
    q = """
    INSERT INTO ohlcv(symbol, timeframe, ts, o, h, l, c, v)
    VALUES($1, $2, to_timestamp($3/1000.0), $4, $5, $6, $7, $8)
    ON CONFLICT(symbol, timeframe, ts) DO UPDATE SET
      o=EXCLUDED.o, h=EXCLUDED.h, l=EXCLUDED.l, c=EXCLUDED.c, v=EXCLUDED.v
    """
    await conn.execute(q, symbol, timeframe, ts_ms, o, h, l, c, v)

async def consume():
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    consumer = AIOKafkaConsumer(
        "klines_raw",
        bootstrap_servers=KAFKA_BROKER,
        enable_auto_commit=True,
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    await consumer.start()
    print("Aggregator started. Listening on topic klines_raw")
    try:
        async for msg in consumer:
            try:
                payload = msg.value
                k = payload.get("kline", {})
                if not k.get("x", False):
                    continue  # only store closed candles
                symbol = payload.get("symbol")
                timeframe = payload.get("timeframe")
                t = int(k["t"])  # open time ms
                o = float(k["o"]) ; h = float(k["h"]) ; l = float(k["l"]) ; c = float(k["c"]) ; v = float(k["v"])
                async with pool.acquire() as conn:
                    await upsert_candle(conn, symbol, timeframe, t, o, h, l, c, v)
            except Exception as e:
                print("process error:", e)
    finally:
        await consumer.stop()
        await pool.close()

if __name__ == "__main__":
    uvloop.run(consume())
