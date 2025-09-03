import os, time, json, logging, re, urllib.parse, datetime
import feedparser
from bs4 import BeautifulSoup
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

KAFKA_BROKER = os.getenv("KAFKA_BROKER","kafka:9092")
TOPIC = os.getenv("NEWS_TOPIC","news_raw")

# nguồn RSS crypto-first (có thể đặt trong .env)
RSS = [s.strip() for s in os.getenv("NEWS_RSS",
    "https://www.coindesk.com/arc/outboundfeeds/rss/?outputType=xml,"
    "https://cointelegraph.com/rss,"
    "https://cryptoslate.com/feed/,"
    "https://cryptonews.com/news/feed/,"
    "https://cryptopotato.com/feed/,"
    "https://bitcoinmagazine.com/news/feed"
).split(",") if s.strip()]

# whitelist domain
ALLOW_DOMAINS = set([d.strip().lower() for d in os.getenv("NEWS_ALLOW_DOMAINS",
    "coindesk.com,cointelegraph.com,cryptoslate.com,cryptonews.com,cryptopotato.com,bitcoinmagazine.com,vnexpress.net,tuoitre.vn,blogtienao.com,tapchibitcoin.io,coin68.com,marginatm.com"
).split(",") if d.strip()])

# từ khóa để lọc
KEYWORDS = [k.strip().lower() for k in os.getenv("NEWS_FILTER_KEYWORDS",
    "crypto,bitcoin,ethereum,btc,eth,sol,defi,web3,blockchain,staking,altcoin,binance,coinbase,etf,stablecoin,tiền số,tiền điện tử,tienso,tien dien tu,tienso"
).split(",") if k.strip()]

# map symbol
SYMBOLS = [s.strip().upper() for s in os.getenv("NEWS_SYMBOLS",
    "BTC,ETH,BNB,ADA,SOL,XRP,DOGE,DOT,AVAX,LTC"
).split(",") if s.strip()]

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER,
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"))

def clean_html(html: str) -> str:
    try:
        soup = BeautifulSoup(html or "", "html.parser")
        return soup.get_text(" ", strip=True)
    except Exception:
        return ""

def host_of(url: str) -> str:
    try:
        host = urllib.parse.urlparse(url).netloc.lower()
        return host[4:] if host.startswith("www.") else host
    except Exception:
        return ""

def source_name_of(url: str) -> str:
    host = host_of(url)
    if not host: return ""
    # lấy phần domain chính làm tên
    parts = host.split(".")
    core = parts[-2] if len(parts) >= 2 else host
    return core.capitalize()

def host_allowed(url: str) -> bool:
    host = host_of(url)
    return any(host.endswith(d) for d in ALLOW_DOMAINS)

def text_match(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in KEYWORDS)

def extract_symbols(text: str):
    res = []
    up = (text or "").upper()
    for s in SYMBOLS:
        if re.search(rf"(?<![A-Z0-9]){re.escape(s)}(?![A-Z0-9])", up):
            res.append(s)
    return res or None

def iso_from_struct(tp):
    try:
        return datetime.datetime.fromtimestamp(time.mktime(tp), tz=datetime.timezone.utc).isoformat()
    except Exception:
        return None

def fetch_rss(url):
    d = feedparser.parse(url)
    out = []
    for e in d.entries:
        title = (e.get("title") or "").strip()
        link = (e.get("link") or "").strip()
        summary = clean_html(e.get("summary") or e.get("description") or "")
        if not link or not host_allowed(link):
            continue

        # timestamp ưu tiên: published_parsed -> updated_parsed -> now()
        published = None
        if e.get("published_parsed"):
            published = iso_from_struct(e.published_parsed)
        if not published and e.get("updated_parsed"):
            published = iso_from_struct(e.updated_parsed)
        if not published:
            published = datetime.datetime.now(datetime.timezone.utc).isoformat()

        text = f"{title}. {summary}"
        if not text_match(text):
            continue

        out.append({
            "source": url,                       # RSS feed url (nguyên gốc)
            "source_name": source_name_of(link), # CoinDesk / Vnexpress / ...
            "title": title,
            "url": link,
            "summary": summary,
            "published": published,              # ISO string
            "symbols": extract_symbols(text),
        })
    return out

def main():
    seen = set()
    interval = int(os.getenv("NEWS_POLL_SECONDS","120"))
    logging.info("News crawler starting. Sources=%d, interval=%ss", len(RSS), interval)
    while True:
        for u in RSS:
            try:
                items = fetch_rss(u)
                for it in items:
                    if not it["url"] or it["url"] in seen:
                        continue
                    seen.add(it["url"])
                    producer.send(TOPIC, it)
                    logging.info("published: [%s] %s", it["source_name"], it["title"][:100])
            except Exception as e:
                logging.warning("fail %s: %s", u, e)
        producer.flush()
        time.sleep(interval)

if __name__ == "__main__":
    main()
