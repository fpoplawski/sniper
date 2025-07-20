CREATE TABLE IF NOT EXISTS weekday_avg (
  origin TEXT NOT NULL,
  destination TEXT NOT NULL,
  weekday INTEGER NOT NULL,
  avg_price NUMERIC NOT NULL,
  PRIMARY KEY (origin, destination, weekday)
);
