import os, sys, time, argparse
import requests, psycopg2

ENV_PATHS = [".env", ".env.example"]

def load_env():
    env = {}
    for p in ENV_PATHS:
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line: continue
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip()
            break
    # defaults (if not in file)
    env.setdefault("POSTGRES_USER", "postgres")
    env.setdefault("POSTGRES_PASSWORD", "postgres")
    env.setdefault("POSTGRES_DB", "market")
    env.setdefault("POSTGRES_PORT", "5432")
    env.setdefault("BINANCE_SYMBOLS", "BTCUSDT,ETHUSDT")
    env.setdefault("BINANCE_TIMEFRAMES", "1m,5m")
    return env

def pg_conn(env):
    dsn = f"dbname={env['POSTGRES_DB']} user={env['POSTGRES_USER']} password={env['POSTGRES_PASSWORD']} host=localhost port={env['POSTGRES_PORT']}"
    return psycopg2.connect(dsn)

def fetch_klines(symbol: str, interval: str, limit: int = 1000, end_time_ms: int | None = None):
    """
    Fetch a batch of klines from Binance REST.
    Returns list of arrays: [openTime, open, high, low, close, volume, closeTime, ...]
    - Binance returns ASC by time (oldest -> newest).
    - If endTime is provided, the last kline will have openTime <= endTime.
    """
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if end_time_ms is not None:
        params["endTime"] = end_time_ms
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def upsert_klines(conn, rows, symbol, tf):
    if not rows: return 0
    sql = """
    INSERT INTO ohlcv(symbol,timeframe,ts,o,h,l,c,v)
    VALUES (%s,%s,TO_TIMESTAMP(%s/1000.0),%s,%s,%s,%s,%s)
    ON CONFLICT(symbol,timeframe,ts) DO UPDATE SET
      o=EXCLUDED.o, h=EXCLUDED.h, l=EXCLUDED.l, c=EXCLUDED.c, v=EXCLUDED.v
    """
    cur = conn.cursor()
    for k in rows:
        cur.execute(sql, (symbol, tf, k[0], k[1], k[2], k[3], k[4], k[5]))
    conn.commit()
    cur.close()
    return len(rows)

def backfill_symbol_tf(conn, symbol, tf, max_bars: int, per_call: int, sleep_ms: int):
    """
    Pull newest -> older using endTime pagination until reaching max_bars or no more data.
    """
    symbol = symbol.upper()
    tf = tf.lower()
    total = 0
    end_time = None  # start from latest

    while total < max_bars:
        # remaining desired bars for this TF
        to_fetch = min(per_call, max_bars - total)
        batch = fetch_klines(symbol, tf, limit=to_fetch, end_time_ms=end_time)
        if not batch:
            break

        inserted = upsert_klines(conn, batch, symbol, tf)
        total += inserted

        # move the end_time cursor to just before the oldest openTime in this batch
        oldest_open_time = batch[0][0]  # ascending order
        end_time = oldest_open_time - 1

        print(f"[{symbol} {tf}] inserted {inserted} (total {total}) — next endTime: {end_time}")
        # be nice to API
        time.sleep(sleep_ms / 1000.0)

        # If Binance ever returns fewer than requested, keep going until empty or max_bars reached

    return total

def main():
    parser = argparse.ArgumentParser(description="Backfill all symbols/timeframes from .env into TimescaleDB")
    parser.add_argument("--max-bars", type=int, default=2000, help="Max candles per symbol/timeframe to fetch (default: 2000)")
    parser.add_argument("--per-call", type=int, default=1000, help="Limit per REST call (<=1000; default: 1000)")
    parser.add_argument("--sleep-ms", type=int, default=200, help="Sleep between REST calls to avoid rate-limit (default: 200ms)")
    parser.add_argument("--symbols", type=str, default=None, help="Override symbols, e.g. BTCUSDT,ETHUSDT")
    parser.add_argument("--timeframes", type=str, default=None, help="Override timeframes, e.g. 1m,5m,15m")
    args = parser.parse_args()

    env = load_env()
    symbols = [s.strip().upper() for s in (args.symbols or env["BINANCE_SYMBOLS"]).split(",") if s.strip()]
    tfs = [t.strip().lower() for t in (args.timeframes or env["BINANCE_TIMEFRAMES"]).split(",") if t.strip()]
    per_call = max(1, min(1000, args.per_call))

    print("Symbols:", symbols)
    print("Timeframes:", tfs)
    print(f"Target: up to {args.max_bars} bars per TF (batch {per_call}, sleep {args.sleep_ms} ms)")

    conn = pg_conn(env)
    try:
        total_all = 0
        for s in symbols:
            for tf in tfs:
                try:
                    n = backfill_symbol_tf(conn, s, tf, args.max_bars, per_call, args.sleep_ms)
                    total_all += n
                except requests.HTTPError as e:
                    print(f"[WARN] HTTP error for {s} {tf}: {e}")
                except Exception as e:
                    print(f"[WARN] Failed {s} {tf}: {e}")
        print("Done. Total rows upserted:", total_all)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
