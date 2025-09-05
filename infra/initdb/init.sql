
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE TABLE IF NOT EXISTS ohlcv (
  symbol TEXT NOT NULL,
  timeframe TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  o NUMERIC NOT NULL,
  h NUMERIC NOT NULL,
  l NUMERIC NOT NULL,
  c NUMERIC NOT NULL,
  v NUMERIC NOT NULL,
  PRIMARY KEY(symbol, timeframe, ts)
);

SELECT create_hypertable('ohlcv', by_range('ts'), if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS ohlcv_symbol_tf_ts_idx ON ohlcv(symbol, timeframe, ts DESC);
