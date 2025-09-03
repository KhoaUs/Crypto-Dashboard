# AI Finance Stack — Minimal MVD

This is a minimal end-to-end skeleton to ingest Binance klines -> Kafka -> TimescaleDB,
and serve candles via a FastAPI service. It's the first deployable slice of the architecture.

## Services
- **postgres (TimescaleDB)**: stores OHLCV
- **zookeeper + kafka**: message bus
- **market-ingestor (Node.js)**: subscribes Binance WS (symbols/timeframes) and publishes `klines_raw` to Kafka
- **candle-aggregator (Python worker)**: consumes `klines_raw`, upserts closed candles to Timescale
- **candle-api (FastAPI)**: serves `GET /candles?symbol=BTCUSDT&tf=1m&limit=1000`

## Quick start
1. Copy env:
   ```bash
   cp .env.example .env
   ```
2. Start:
   ```bash
   docker compose up --build
   ```
3. Check health:
   - Candle API: http://localhost:8000/health
   - Query candles: http://localhost:8000/candles?symbol=BTCUSDT&tf=1m&limit=100

> Note: Some ISPs may block Binance WS. If ingestor can't connect, try a different network/VPN.

## Schema
- `ohlcv(symbol TEXT, timeframe TEXT, ts TIMESTAMPTZ, o NUMERIC, h NUMERIC, l NUMERIC, c NUMERIC, v NUMERIC)`
Primary key: (symbol, timeframe, ts). Timescale hypertable on `ts`.
