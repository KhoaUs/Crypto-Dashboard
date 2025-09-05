import numpy as np
import pandas as pd
import requests
from typing import Dict, List, Tuple, Optional
import jwt
import os
import datetime as dt

# Lấy secret chung qua ENV; nếu bạn chưa bật auth cho candle-api thì header này vô hại
JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-me")
ALGO = "HS256"

def _service_access_token() -> str:
    """Tạo JWT nội bộ cho service→service; hạn 10 phút."""
    exp = dt.datetime.utcnow() + dt.timedelta(minutes=10)
    return jwt.encode({"sub": "backtest-service", "role": "ADMIN", "exp": exp}, JWT_SECRET, algorithm=ALGO)

def load_candles(candle_api: str, symbol: str, tf: str, start: Optional[int], end: Optional[int], limit: int) -> pd.DataFrame:
    """
    Lấy dữ liệu từ Candle API.
    - Nếu có start/end (epoch seconds), sẽ lọc sau khi tải (tải rộng bằng limit).
    - Nếu không, dùng limit gần nhất.
    """
    url = f"{candle_api}/candles?symbol={symbol}&tf={tf}&limit={limit}"

    # Quan trọng: gắn Bearer token để vượt qua bảo vệ JWT của candle-api
    headers = {"Authorization": f"Bearer {_service_access_token()}"}

    resp = requests.get(url, timeout=10, headers=headers)
    # Nếu candle-api trả lỗi (vd 401/403/500), raise luôn để bạn thấy nguyên nhân thật trong log
    resp.raise_for_status()

    arr = resp.json()
    df = pd.DataFrame(arr)
    if df.empty:
        return df

    df = df.rename(columns={"ts":"time","o":"open","h":"high","l":"low","c":"close","v":"volume"})
    df = df[["time","open","high","low","close","volume"]].copy()

    if start is not None:
        df = df[df["time"] >= start]
    if end is not None:
        df = df[df["time"] <= end]

    df = df.sort_values("time").reset_index(drop=True)
    return df

def sma(series: pd.Series, n: int) -> pd.Series:
    return series.rolling(n, min_periods=n).mean()

def crossings(a: pd.Series, b: pd.Series) -> pd.Series:
    """+1 khi a cắt lên b, -1 khi a cắt xuống b, 0 otherwise."""
    sgn_prev = np.sign(a.shift(1) - b.shift(1))
    sgn_now  = np.sign(a - b)
    up = (sgn_prev <= 0) & (sgn_now > 0)
    dn = (sgn_prev >= 0) & (sgn_now < 0)
    out = pd.Series(0, index=a.index)
    out[up] = 1
    out[dn] = -1
    return out

def load_sentiment(news_api: str, base_symbol: str, limit: int = 50) -> List[Dict]:
    url = f"{news_api}/news?symbol={base_symbol}&limit={limit}"
    try:
        return requests.get(url, timeout=8).json()
    except Exception:
        return []

def aggregate_sent_last_window(news_items: List[Dict], window_sec: int, end_ts: int) -> float:
    if not news_items:
        return 0.0
    from_ts = end_ts - window_sec
    vals = [float(x.get("sentiment") or 0.0) for x in news_items
            if isinstance(x.get("published_at"), (int, float)) and from_ts <= x["published_at"] <= end_ts]
    if not vals:
        return 0.0
    return float(np.mean(vals))

def symbol_to_base(sym: str) -> str:
    sym = sym.upper()
    for base in ("USDT","USD","BUSD","USDC","FDUSD","TUSD"):
        if sym.endswith(base): return sym[:-len(base)]
    return sym

def backtest_ma_cross(
    df: pd.DataFrame, fee: float = 0.0005
) -> Tuple[List[Dict], Dict]:
    """
    Chiến lược: MA30 cắt MA90 -> vào/đảo vị thế:
      - +1 khi cắt lên (BUY)
      - 0 khi cắt xuống (đóng vị thế)
    Đơn giản: chỉ long/flat. (Có thể mở rộng short sau)
    """
    df = df.copy()
    df["sma30"] = sma(df["close"], 30)
    df["sma90"] = sma(df["close"], 90)
    df["signal"] = crossings(df["sma30"], df["sma90"])
    df = df.dropna().reset_index(drop=True)

    trades = []
    pos = 0
    entry_price = None

    for i, row in df.iterrows():
        t = int(row["time"])
        price = float(row["close"])
        sig = int(row["signal"])

        if pos == 0 and sig == +1:
            # open long
            entry_price = price * (1 + fee)  # trượt + phí
            pos = 1
            trades.append({"time": t, "action": "BUY", "price": price})
        elif pos == 1 and sig == -1:
            # close long
            exit_price = price * (1 - fee)
            pnl = (exit_price - entry_price) / entry_price
            trades.append({"time": t, "action": "SELL", "price": price, "pnl": pnl})
            pos = 0
            entry_price = None

    # nếu còn vị thế, đóng ở cây cuối để tính PnL
    if pos == 1 and entry_price is not None:
        last_price = float(df.iloc[-1]["close"]) * (1 - fee)
        pnl = (last_price - entry_price) / entry_price
        trades.append({"time": int(df.iloc[-1]["time"]), "action": "SELL", "price": float(df.iloc[-1]["close"]), "pnl": pnl})
        pos = 0
        entry_price = None

    # tính tổng hợp
    pnls = [x["pnl"] for x in trades if "pnl" in x]
    summary = {
        "n_trades": len(pnls),
        "win": int(sum(1 for p in pnls if p > 0)),
        "loss": int(sum(1 for p in pnls if p <= 0)),
        "winrate": (sum(1 for p in pnls if p > 0) / len(pnls)) if pnls else 0.0,
        "total_return": float(np.sum(pnls)) if pnls else 0.0,
    }
    return trades, summary

def backtest_ma_cross_sent(
    df: pd.DataFrame, news_api: str, symbol: str, window_sec: int = 15*60, fee: float = 0.0005
) -> Tuple[List[Dict], Dict]:
    """
    MA30 cắt MA90 và sentiment trung bình trong window gần nhất >= 0 mới BUY.
    SELL khi cắt xuống như bình thường.
    """
    df = df.copy()
    df["sma30"] = sma(df["close"], 30)
    df["sma90"] = sma(df["close"], 90)
    df["signal_raw"] = crossings(df["sma30"], df["sma90"])
    df = df.dropna().reset_index(drop=True)

    base = symbol_to_base(symbol)
    news_items = load_sentiment(news_api, base, limit=100)

    trades = []
    pos = 0
    entry_price = None

    for i, row in df.iterrows():
        t = int(row["time"])
        price = float(row["close"])
        sig = int(row["signal_raw"])

        if pos == 0 and sig == +1:
            sent_mean = aggregate_sent_last_window(news_items, window_sec, t)
            if sent_mean >= 0.0:
                entry_price = price * (1 + fee)
                pos = 1
                trades.append({"time": t, "action": "BUY", "price": price, "sent_mean": sent_mean})
        elif pos == 1 and sig == -1:
            exit_price = price * (1 - fee)
            pnl = (exit_price - entry_price) / entry_price
            trades.append({"time": t, "action": "SELL", "price": price, "pnl": pnl})
            pos = 0
            entry_price = None

    if pos == 1 and entry_price is not None:
        last_price = float(df.iloc[-1]["close"]) * (1 - fee)
        pnl = (last_price - entry_price) / entry_price
        trades.append({"time": int(df.iloc[-1]["time"]), "action": "SELL", "price": float(df.iloc[-1]["close"]), "pnl": pnl})
        pos = 0
        entry_price = None

    pnls = [x["pnl"] for x in trades if "pnl" in x]
    summary = {
        "n_trades": len(pnls),
        "win": int(sum(1 for p in pnls if p > 0)),
        "loss": int(sum(1 for p in pnls if p <= 0)),
        "winrate": (sum(1 for p in pnls if p > 0) / len(pnls)) if pnls else 0.0,
        "total_return": float(np.sum(pnls)) if pnls else 0.0,
    }
    return trades, summary
