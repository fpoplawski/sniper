CREATE TABLE IF NOT EXISTS weekday_avg (
    origin TEXT,
    destination TEXT,
    weekday INTEGER,
    avg_price NUMERIC,
    PRIMARY KEY (origin, destination, weekday)
);
