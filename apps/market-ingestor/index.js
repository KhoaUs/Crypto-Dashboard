import WebSocket from 'ws';
import { Kafka } from 'kafkajs';

const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const SYMBOLS = (process.env.BINANCE_SYMBOLS || 'BTCUSDT').split(',').map(s => s.trim().toLowerCase());
const TFS = (process.env.BINANCE_TIMEFRAMES || '1m').split(',').map(s => s.trim().toLowerCase());

const kafka = new Kafka({ clientId: 'market-ingestor', brokers: [KAFKA_BROKER] });
const producer = kafka.producer();

function buildStreamPath() {
  const streams = [];
  for (const s of SYMBOLS) {
    for (const tf of TFS) {
      streams.push(`${s}@kline_${tf}`);
    }
  }
  return streams.join('/');
}

async function run() {
  await producer.connect();
  const path = buildStreamPath();
  const url = `wss://stream.binance.com:9443/stream?streams=${path}`;
  console.log('Connecting to', url);
  const ws = new WebSocket(url);

  ws.on('open', () => console.log('WS connected'));
  ws.on('close', () => console.log('WS closed'));
  ws.on('error', (err) => console.error('WS error', err));

  ws.on('message', async (msg) => {
    try {
      const obj = JSON.parse(msg.toString());
      const payload = obj.data;
      if (!payload || !payload.k) return;
      const k = payload.k;
      const out = {
        symbol: k.s,                 // e.g. "BTCUSDT"
        timeframe: k.i,              // e.g. "1m"
        kline: k,                    // original kline payload
        ts_event: payload.E || Date.now()
      };
      await producer.send({
        topic: 'klines_raw',
        messages: [{ value: JSON.stringify(out) }]
      });
    } catch (e) {
      console.error('Parse/send error', e);
    }
  });
}

run().catch(e => {
  console.error(e);
  process.exit(1);
});
