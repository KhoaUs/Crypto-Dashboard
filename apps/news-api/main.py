import os, asyncpg
from fastapi import FastAPI, Depends, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware   
from typing import Optional

DATABASE_URL = os.getenv("DATABASE_URL","postgresql://postgres:postgres@postgres:5432/market")
app = FastAPI(title="News API")

# --- CORS cho frontend/local file/Live Server ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # hoặc chặt chẽ hơn: ["http://localhost:5500","http://127.0.0.1:5500"]
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
# ------------------------------------------------

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
    return {"status":"ok"}

@app.get("/news")
async def news(q: Optional[str]=None, symbol: Optional[str]=None, limit: int=50):
    where_parts = []
    params = []
    idx = 0

    def add_param(val):
        nonlocal idx
        params.append(val)
        idx += 1
        return f"${idx}"

    if q:
        p1 = add_param(f"%{q}%")
        p2 = add_param(f"%{q}%")
        where_parts.append(f"(title ILIKE {p1} OR summary ILIKE {p2})")

    if symbol:
        p3 = add_param(symbol.upper())
        where_parts.append(f"{p3} = ANY(symbols)")

    p_lim = add_param(limit)
    where_sql = "WHERE " + " AND ".join(where_parts) if where_parts else ""

    sql = f"""
    SELECT id, source, source_name, title, url, summary, lang,
           EXTRACT(EPOCH FROM published_at)::bigint AS published_at,
           COALESCE(sentiment, 0)::float AS sentiment,
           sentiment_label, symbols
    FROM news_items
    {where_sql}
    ORDER BY published_at DESC NULLS LAST
    LIMIT {p_lim}
    """

    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)

    return JSONResponse(content=[dict(r) for r in rows])
