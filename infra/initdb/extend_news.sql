-- ============================================
-- News + Sentiment schema (idempotent)
-- ============================================

BEGIN;

-- Bảng lưu bài viết + sentiment
CREATE TABLE IF NOT EXISTS news_items (
  id             BIGSERIAL PRIMARY KEY,
  source         TEXT        NOT NULL,          -- RSS feed URL gốc
  source_name    TEXT,                          -- Tên nguồn (CoinDesk, VnExpress, ...)
  title          TEXT        NOT NULL,          -- Tiêu đề
  url            TEXT        NOT NULL UNIQUE,   -- Link bài (duy nhất)
  summary        TEXT,                          -- Tóm tắt/ngắn gọn
  lang           TEXT,                          -- Ngôn ngữ (vi/en/...)
  published_at   TIMESTAMPTZ,                   -- Thời điểm publish (UTC)
  fetched_at     TIMESTAMPTZ DEFAULT NOW(),     -- Thời điểm crawler fetch
  sentiment      NUMERIC,                       -- Điểm -1..+1
  sentiment_label TEXT,                         -- positive/neutral/negative
  symbols        TEXT[]                         -- Mảng symbol: {BTC,ETH,...}
);

-- Bổ sung cột nếu dựng DB cũ (an toàn khi chạy nhiều lần)
ALTER TABLE news_items
  ADD COLUMN IF NOT EXISTS source_name TEXT;

-- Chỉ mục phục vụ truy vấn
-- Lấy tin mới nhất
CREATE INDEX IF NOT EXISTS news_pub_idx     ON news_items (published_at DESC);
-- Lọc theo nguồn
CREATE INDEX IF NOT EXISTS news_source_idx  ON news_items (source);
-- Lọc theo symbol (mảng) nhanh hơn
CREATE INDEX IF NOT EXISTS news_symbols_idx ON news_items USING GIN (symbols);

COMMIT;
