from __future__ import annotations

import sqlite3
from collections import defaultdict

from db import DB_FILE, upsert_daily_avg


def aggregate(db_path: str = DB_FILE) -> None:
    """Compute 30-day average price per route and store in ``offers_agg``."""
    conn = sqlite3.connect(db_path)
    cur = conn.execute(
        """
        SELECT origin, destination,
               DATE(fetched_at) AS day,
               MIN(price_pln) AS min_price
          FROM offers_raw
         WHERE fetched_at >= DATE('now', '-30 days')
         GROUP BY origin, destination, day
        """
    )
    data: defaultdict[tuple[str, str], list[float]] = defaultdict(list)
    for origin, dest, _day, min_price in cur.fetchall():
        try:
            data[(origin, dest)].append(float(min_price))
        except (TypeError, ValueError):
            continue
    conn.close()

    for (origin, dest), prices in data.items():
        if not prices:
            continue
        mean_price = sum(prices) / len(prices)
        upsert_daily_avg(origin, dest, mean_price, db_path=db_path)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM offers_agg WHERE day < DATE('now', '-60 days')"
    )
    conn.commit()
    conn.close()


def main() -> None:
    aggregate()


if __name__ == "__main__":
    main()
