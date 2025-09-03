import os, time
import requests, psycopg2

def env_get(key, default=None):
    v = os.getenv(key)
    return v if (v is not None and v != "") else default

# đọc cấu hình từ ENV (được compose truyền vào)
PG_USER = env_get("POSTGRES_USER", "postgres")
PG_PASS = env_get("POSTGRES_PASSWORD", "postgres")
PG_DB   = env_get("POSTGRES_DB", "market")
PG_PORT = env_get("POSTGRES_PORT", "5432")
PG_HOST = env_get("POSTGRES_HOST", "postgres")  # dùng hostname của service postgres trong compose

SYMBOLS = [s.strip().upper() for s in env_get("BINANCE_SYMBOLS", "BTCUSDT,ETHUSDT").split(",") if s.strip()]
TFS      = [t.strip().lower() for t in env_get("BINANCE_TIMEFRAMES", "1m,5m").split(",") if t.strip()]

MAX_BARS = int(env_get("BACKFILL_MAX_BARS", "2000"))
PER_CALL = max(1, min(1000, int(env_get("BACKFILL_PER_CALL", "1000"))))
SLEEP_MS = int(env_get("BACKFILL_SLEEP_MS", "200"))

DSN = f"dbname={PG_DB} user={PG_USER} password={PG_PASS} host={PG_HOST} port={PG_PORT}"

def fetch_klines(symbol: str, interval: str, limit: int = 1000, end_time_ms: int | None = None):
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if end_time_ms is not None:
        params["endTime"] = end_time_ms
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()  # asc by time

def upsert(conn, rows, symbol, tf):
    if not rows: return 0
    sql = """
    INSERT INTO ohlcv(symbol, timeframe, ts, o, h, l, c, v)
    VALUES(%s,%s,TO_TIMESTAMP(%s/1000.0),%s,%s,%s,%s,%s)
    ON CONFLICT(symbol, timeframe, ts) DO UPDATE SET
      o=EXCLUDED.o, h=EXCLUDED.h, l=EXCLUDED.l, c=EXCLUDED.c, v=EXCLUDED.v
    """
    cur = conn.cursor()
    for k in rows:
        cur.execute(sql, (symbol, tf, k[0], k[1], k[2], k[3], k[4], k[5]))
    conn.commit()
    cur.close()
    return len(rows)

def backfill_symbol_tf(conn, symbol, tf, max_bars, per_call, sleep_ms):
    total = 0
    end_time = None
    while total < max_bars:
        batch = fetch_klines(symbol, tf, limit=min(per_call, max_bars - total), end_time_ms=end_time)
        if not batch:
            break
        n = upsert(conn, batch, symbol, tf)
        total += n
        oldest_open_time = batch[0][0]
        end_time = oldest_open_time - 1
        print(f"[{symbol} {tf}] inserted {n} (total {total}) — next endTime {end_time}")
        time.sleep(sleep_ms / 1000.0)
    return total

def main():
    print("Backfill job starting...")
    print("Symbols:", SYMBOLS)
    print("Timeframes:", TFS)
    print(f"Target up to {MAX_BARS} bars/TF (batch {PER_CALL}, sleep {SLEEP_MS} ms)")

    # chờ Postgres sẵn sàng (compose đã có healthcheck, nhưng chờ thêm cho chắc)
    time.sleep(2)

    conn = None
    for _ in range(30):
        try:
            conn = psycopg2.connect(DSN)
            break
        except Exception as e:
            print("Waiting for DB...", e)
            time.sleep(2)
    if conn is None:
        raise RuntimeError("Cannot connect to Postgres")

    try:
        total_all = 0
        for s in SYMBOLS:
            for tf in TFS:
                try:
                    total_all += backfill_symbol_tf(conn, s, tf, MAX_BARS, PER_CALL, SLEEP_MS)
                except requests.HTTPError as e:
                    print(f"[WARN] HTTP for {s} {tf}: {e}")
                except Exception as e:
                    print(f"[WARN] Failed {s} {tf}: {e}")
        print("Backfill done. Total rows upserted:", total_all)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
