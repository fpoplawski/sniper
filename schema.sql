-- offers_raw: każda znaleziona oferta
CREATE TABLE IF NOT EXISTS offers_raw (
  id INTEGER PRIMARY KEY,
  origin TEXT, destination TEXT,
  depart_date DATE, return_date DATE,
  price_pln NUMERIC, airline TEXT,
  stops INTEGER, total_time_h REAL, layover_h REAL,
  deep_link TEXT, fetched_at DATETIME,
  alert_sent INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_offers_route
  ON offers_raw (origin, destination, depart_date);

CREATE INDEX IF NOT EXISTS idx_offers_alert
  ON offers_raw (alert_sent);

-- offers_agg: średnie 30‑dniowe
CREATE TABLE IF NOT EXISTS offers_agg (
  origin TEXT, destination TEXT,
  day DATE,
  mean_price NUMERIC,
  PRIMARY KEY (origin, destination, day)
);
