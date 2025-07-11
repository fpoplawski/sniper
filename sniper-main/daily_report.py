from __future__ import annotations

import sqlite3
import html

from db import DB_FILE
from notifier import send_email_daily


def send_daily_report(db_path: str = DB_FILE) -> None:
    """Send daily summary email for STEAL deals."""
    conn = sqlite3.connect(db_path)
    cur = conn.execute(
        """
        SELECT origin, destination, depart_date, return_date, price_pln, deep_link
          FROM offers_raw
         WHERE alert_sent = 1
           AND fetched_at >= DATE('now', '-1 day')
         ORDER BY price_pln ASC
        """
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return

    body = ["<h2>STEAL deals – ostatnie 24 h</h2><table border=1>"]
    body.append("<tr><th>Route</th><th>Dates</th><th>Price</th></tr>")
    for origin, dest, dep, ret, price, link in rows:
        route = f"{origin}&nbsp;→&nbsp;{dest}"
        dates = f"{dep}&nbsp;→&nbsp;{ret}"
        body.append(
            f"<tr><td>{route}</td><td>{dates}</td><td><a href='{html.escape(link)}'>{price} PLN</a></td></tr>"
        )
    body.append("</table>")
    send_email_daily("".join(body))


def main() -> None:
    send_daily_report()


if __name__ == "__main__":
    main()
