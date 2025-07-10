from __future__ import annotations

from datetime import date
from email.message import EmailMessage
import smtplib
from typing import List, Dict


def send_email(
    deals: List[Dict],
    smtp_host: str,
    smtp_user: str,
    smtp_pass: str,
    to_addr: str,
    *,
    port: int = 465,
    use_tls: bool = False,
) -> None:
    """Send ``deals`` list as an email to ``to_addr``.

    Parameters ``smtp_host``, ``smtp_user`` and ``smtp_pass`` are used for SMTP
    authentication.  Set ``use_tls`` to ``True`` for ``STARTTLS`` connection and
    ``port`` to the appropriate port if different from the default 465.
    """
    if not deals:
        return

    today = date.today()
    subject = f"\U0001F6EB {len(deals)} nowych okazji lotniczych – {today:%Y-%m-%d}"

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = to_addr

    lines: List[str] = []
    for deal in deals:
        origin = deal.get("origin", "")
        destination = deal.get("destination", "")
        route = f"{origin} → {destination}" if origin or destination else ""

        depart = deal.get("depart_date") or deal.get("depart") or ""
        return_date = deal.get("return_date")
        date_str = depart
        if return_date:
            date_str += f" – {return_date}"

        price = deal.get("price", "")
        link = deal.get("deep_link") or deal.get("link") or ""

        lines.append(f"{route} | {date_str} | {price} | {link}")

    text_body = "\n".join(lines)

    html_rows = [
        "<table>",
        "<thead><tr><th>Trasa</th><th>Data</th><th>Cena</th><th>Link</th></tr></thead>",
        "<tbody>",
    ]
    for deal in deals:
        origin = deal.get("origin", "")
        destination = deal.get("destination", "")
        route = f"{origin} → {destination}" if origin or destination else ""

        depart = deal.get("depart_date") or deal.get("depart") or ""
        return_date = deal.get("return_date")
        date_str = depart
        if return_date:
            date_str += f" – {return_date}"

        price = deal.get("price", "")
        link = deal.get("deep_link") or deal.get("link") or ""
        link_html = f'<a href="{link}">{link}</a>' if link else ""

        html_rows.append(
            f"<tr><td>{route}</td><td>{date_str}</td><td>{price}</td><td>{link_html}</td></tr>"
        )
    html_rows.append("</tbody></table>")
    html_body = "\n".join(html_rows)

    msg.set_content(text_body)
    msg.add_alternative(f"<html><body>{html_body}</body></html>", subtype="html")

    if use_tls:
        with smtplib.SMTP(smtp_host, port) as smtp:
            smtp.starttls()
            if smtp_user:
                smtp.login(smtp_user, smtp_pass)
            smtp.send_message(msg)
    else:
        with smtplib.SMTP_SSL(smtp_host, port) as smtp:
            if smtp_user:
                smtp.login(smtp_user, smtp_pass)
            smtp.send_message(msg)


__all__ = ["send_email"]
