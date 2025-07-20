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

-- prevent duplicate offers
CREATE UNIQUE INDEX IF NOT EXISTS idx_offers_unique
  ON offers_raw (
    origin,
    destination,
    depart_date,
    return_date,
    price_pln,
    airline,
    stops,
    deep_link
  );

-- offers_agg: średnie 30‑dniowe
CREATE TABLE IF NOT EXISTS offers_agg (
  origin TEXT, destination TEXT,
  day DATE,
  mean_price NUMERIC,
  PRIMARY KEY (origin, destination, day)
);

-- ① Tabela parowanych pseudo-RT (dwie nogi OW)
CREATE TABLE IF NOT EXISTS offers_pair (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    out_id          INTEGER NOT NULL REFERENCES offers_raw(id) ON DELETE CASCADE,
    in_id           INTEGER NOT NULL REFERENCES offers_raw(id) ON DELETE CASCADE,
    origin          TEXT NOT NULL,
    destination     TEXT NOT NULL,
    depart_date     DATE NOT NULL,
    return_date     DATE NOT NULL,
    price_total_pln NUMERIC NOT NULL,
    steal_pair      INTEGER NOT NULL DEFAULT 0,
    fetched_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(out_id, in_id)
);
CREATE INDEX IF NOT EXISTS pair_dates_idx
    ON offers_pair(origin,destination,depart_date);

-- weekday averages
CREATE TABLE IF NOT EXISTS weekday_avg (
  origin TEXT NOT NULL,
  destination TEXT NOT NULL,
  weekday INTEGER NOT NULL,
  avg_price NUMERIC NOT NULL,
  PRIMARY KEY (origin,destination,weekday)
);
