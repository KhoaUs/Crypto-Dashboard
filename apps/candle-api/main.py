import os
import asyncpg
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional

from fastapi.middleware.cors import CORSMiddleware

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/market")

app = FastAPI(title="Candle API")

# Cho phép gọi từ Live Server (VS Code) hoặc localhost các port phổ biến
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"http://(localhost|127\.0\.0\.1)(:\d+)?",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

pool: Optional[asyncpg.Pool] = None

@app.on_event("startup")
async def startup():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

@app.on_event("shutdown")
async def shutdown():
    if pool:
        await pool.close()

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/candles")
async def candles(symbol: str, tf: str, limit: int = 1000):
    symbol = symbol.upper()
    tf = tf.lower()
    if limit < 1 or limit > 5000:
        raise HTTPException(status_code=400, detail="limit must be 1..5000")

    q = '''
    SELECT EXTRACT(EPOCH FROM ts)::BIGINT as ts,
           o::float, h::float, l::float, c::float, v::float
    FROM ohlcv
    WHERE symbol=$1 AND timeframe=$2
    ORDER BY ts DESC
    LIMIT $3
    '''
    async with pool.acquire() as conn:
        rows = await conn.fetch(q, symbol, tf, limit)
    data = [dict(r) for r in reversed(rows)]
    return JSONResponse(content=data)
