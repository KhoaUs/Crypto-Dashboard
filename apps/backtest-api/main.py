import os
from fastapi import FastAPI, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
from strategies import load_candles, backtest_ma_cross, backtest_ma_cross_sent
from metrics import equity_from_trades, sharpe_approx

CANDLE_API = os.getenv("CANDLE_API_URL", "http://candle-api:8000")
NEWS_API   = os.getenv("NEWS_API_URL",   "http://news-api:8100")

app = FastAPI(title="Backtest API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"status":"ok"}

@app.get("/strategies")
def strategies():
    return [
        {"id":"ma_cross", "name":"MA30 cross MA90 (long/flat)"},
        {"id":"ma_cross_sent", "name":"MA30 cross MA90 + Sentiment>=0 (long/flat)"},
    ]

@app.get("/backtest")
def backtest(
    symbol: str = Query(..., example="BTCUSDT"),
    tf: str = Query("1m", example="1m"),
    rule: str = Query("ma_cross", enum=["ma_cross","ma_cross_sent"]),
    start: Optional[int] = Query(None, description="epoch seconds"),
    end: Optional[int]   = Query(None, description="epoch seconds"),
    limit: int = Query(1500, ge=200, le=5000),
    fee: float = Query(0.0005, ge=0.0, le=0.01)
):
    df = load_candles(CANDLE_API, symbol, tf, start, end, limit)
    if df.empty or len(df) < 120:
        return {"error":"not-enough-candles"}

    if rule == "ma_cross":
        trades, summary = backtest_ma_cross(df, fee=fee)
    else:
        trades, summary = backtest_ma_cross_sent(df, news_api=NEWS_API, symbol=symbol, fee=fee)

    curve = equity_from_trades(trades, start_eq=1.0)
    summary.update({
        "equity_end": curve[-1] if curve else 1.0,
        "sharpe_like": sharpe_approx(trades),
        "symbol": symbol,
        "tf": tf,
        "rule": rule
    })
    return {"summary": summary, "trades": trades}
