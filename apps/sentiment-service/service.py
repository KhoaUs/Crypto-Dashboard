import os, json, asyncio, logging, datetime, re
import asyncpg
from aiokafka import AIOKafkaConsumer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/market")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("NEWS_TOPIC", "news_raw")

analyzer = SentimentIntensityAnalyzer()

async def upsert_news(conn, item):
    q = """
    INSERT INTO news_items(source, source_name, title, url, summary, lang, published_at, sentiment, sentiment_label, symbols)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
    ON CONFLICT (url) DO UPDATE SET
      source_name=EXCLUDED.source_name,
      summary=EXCLUDED.summary,
      lang=EXCLUDED.lang,
      published_at=EXCLUDED.published_at,
      sentiment=EXCLUDED.sentiment,
      sentiment_label=EXCLUDED.sentiment_label,
      symbols=EXCLUDED.symbols
    """
    await conn.execute(q,
        item.get("source"),
        item.get("source_name"),
        item.get("title"),
        item.get("url"),
        item.get("summary"),
        item.get("lang"),
        item.get("published_at"),
        float(item.get("sentiment") or 0.0),
        item.get("sentiment_label"),
        item.get("symbols"),
    )

def detect_lang(text: str) -> str:
    # đơn giản: có dấu tiếng Việt thì 'vi', không thì 'en'
    if re.search(r"[ăâđêôơưáàảãạắằẳẵặấầẩẫậéèẻẽẹếềểễệíìỉĩịóòỏõọốồổỗộớờởỡợúùủũụứừửữựýỳỷỹỵ]", (text or "").lower()):
        return "vi"
    return "en"

def score_text(text: str):
    vs = analyzer.polarity_scores(text or "")
    comp = vs.get("compound", 0.0)
    if comp >= 0.05: label = "positive"
    elif comp <= -0.05: label = "negative"
    else: label = "neutral"
    return comp, label

async def run():
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        enable_auto_commit=True,
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="sentiment-service",
    )
    await consumer.start()
    logging.info("Sentiment service started. Consuming topic=%s", TOPIC)
    try:
        async for msg in consumer:
            d = msg.value  # payload từ crawler
            title = (d.get("title") or "").strip()
            summary = (d.get("summary") or "").strip()
            text = f"{title}. {summary}".strip()
            lang = detect_lang(text)
            score, label = score_text(text)

            pub = d.get("published")
            try:
                published_at = datetime.datetime.fromisoformat(pub.replace("Z", "+00:00")) if pub else None
            except Exception:
                published_at = None

            item = {
                "source": d.get("source"),
                "source_name": d.get("source_name") or "",
                "title": title,
                "url": d.get("url"),
                "summary": summary,
                "lang": lang,
                "published_at": published_at,
                "sentiment": score,
                "sentiment_label": label,
                "symbols": d.get("symbols") or None,
            }

            try:
                async with pool.acquire() as conn:
                    await upsert_news(conn, item)
                logging.info("stored: [%s] %s", item["source_name"], item["title"][:100])
            except Exception as e:
                logging.error("DB error: %s", e)
    finally:
        await consumer.stop()
        await pool.close()

if __name__ == "__main__":
    asyncio.run(run())
