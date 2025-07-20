from __future__ import annotations

import logging
import sqlite3
import tempfile
from typing import Optional, Union

import pandas as pd

from .db import DB_FILE, upsert_daily_avg

logger = logging.getLogger(__name__)


def aggregate(
    db_path: str = DB_FILE, *, output: Optional[str] = None
) -> Union[pd.DataFrame, str, None]:
    """Compute 30-day average price per route using pandas.

    Parameters
    ----------
    db_path:
        Path to the SQLite database.
    output:
        ``None``     – return ``None`` (default, backwards compatible).
        ``"df"``     – return ``pandas.DataFrame`` with results.
        ``"csv"``    – write DataFrame to a temporary CSV and return its path.
    """

    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(
            """
            SELECT origin, destination, DATE(fetched_at) AS day, price_pln
              FROM offers_raw
             WHERE fetched_at >= DATE('now', '-30 days')
            """,
            conn,
            parse_dates=["day"],
        )
    finally:
        conn.close()

    if df.empty:
        result_df = pd.DataFrame(
            columns=["origin", "destination", "day", "mean_price"]
        )
    else:
        daily_min = df.groupby(
            ["origin", "destination", "day"], as_index=False
        )["price_pln"].min()
        daily_min = daily_min.sort_values("day")
        rolling = (
            daily_min.groupby(["origin", "destination"], as_index=False)
            .apply(
                lambda g: g.assign(
                    mean_price=g["price_pln"]
                    .rolling(window=30, min_periods=1)
                    .mean()
                )
            )
            .reset_index(drop=True)
        )
        result_df = (
            rolling.sort_values("day")
            .groupby(["origin", "destination"], as_index=False)
            .tail(1)[["origin", "destination", "day", "mean_price"]]
        )

    for row in result_df.itertuples(index=False):
        upsert_daily_avg(
            row.origin, row.destination, row.mean_price, db_path=db_path
        )

    # Remove old aggregated records (>60 days) using pandas
    conn = sqlite3.connect(db_path)
    try:
        agg_df = pd.read_sql_query(
            "SELECT origin, destination, day, mean_price FROM offers_agg",
            conn,
            parse_dates=["day"],
        )
        cutoff = pd.Timestamp.utcnow().tz_localize(
            None
        ).normalize() - pd.Timedelta(days=60)
        filtered = agg_df[agg_df["day"] >= cutoff]
        conn.execute("DELETE FROM offers_agg")
        if not filtered.empty:
            filtered.to_sql(
                "offers_agg", conn, if_exists="append", index=False
            )
        conn.commit()
    finally:
        conn.close()

    if output == "df":
        return result_df.reset_index(drop=True)
    if output == "csv":
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        result_df.to_csv(tmp.name, index=False)
        return tmp.name
    return None


def compute_weekday_averages(db_path: str = DB_FILE) -> pd.DataFrame:
    """Return average price per route and weekday over the last 90 days."""

    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(
            """
            SELECT origin, destination, depart_date, price_pln
              FROM offers_raw
             WHERE fetched_at >= DATE('now', '-90 days')
            """,
            conn,
            parse_dates=["depart_date"],
        )
    finally:
        conn.close()

    if df.empty:
        return pd.DataFrame(
            columns=["origin", "destination", "weekday", "avg_price"]
        )

    df["weekday"] = df["depart_date"].dt.weekday
    result_df = (
        df.groupby(["origin", "destination", "weekday"], as_index=False)[
            "price_pln"
        ]
        .mean()
        .rename(columns={"price_pln": "avg_price"})
    )
    return result_df


def store_weekday_averages(db_path: str = DB_FILE) -> None:
    """Compute weekday averages and store them in ``weekday_avg`` table."""

    df = compute_weekday_averages(db_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("DELETE FROM weekday_avg")
        if not df.empty:
            df.to_sql("weekday_avg", conn, if_exists="append", index=False)
        conn.commit()
    finally:
        conn.close()


def main() -> None:
    aggregate()


if __name__ == "__main__":
    main()
