from telegram import Bot
import smtplib
import ssl
from email.mime.text import MIMEText
from .config import Config

import logging

# Load configuration once
cfg = Config.from_json()

logger = logging.getLogger(__name__)

bot = Bot(token=cfg.telegram_bot_token)


def send_telegram(msg: str) -> None:
    """Send *msg* via Telegram if enabled in configuration."""
    if cfg.telegram_instant:
        bot.send_message(
            chat_id=cfg.telegram_chat_id,
            text=msg,
            parse_mode="Markdown",
        )


def send_email_daily(html_body: str) -> None:
    """Send daily email with *html_body* if enabled."""
    if not cfg.email_daily:
        return
    msg = MIMEText(html_body, "html")
    msg["Subject"] = "STEAL Flight Deals – last 24h"
    msg["From"] = cfg.email_from
    msg["To"] = cfg.email_to
    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(cfg.smtp_host, cfg.smtp_port, context=ctx) as s:
        s.login(cfg.smtp_user, cfg.smtp_pass)
        s.send_message(msg)
