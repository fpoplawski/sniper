from telegram import Bot
import smtplib
import ssl
from email.mime.text import MIMEText
from config import Config
from db import mark_alert_sent

bot = Bot(token=Config.telegram_bot_token)


def send_telegram(msg: str) -> None:
    """Send *msg* via Telegram if enabled in configuration."""
    if Config.telegram_instant:
        bot.send_message(
            chat_id=Config.telegram_chat_id,
            text=msg,
            parse_mode="Markdown",
        )


def send_email_daily(html_body: str) -> None:
    """Send daily email with *html_body* if enabled."""
    if not Config.email_daily:
        return
    msg = MIMEText(html_body, "html")
    msg["Subject"] = "STEAL Flight Deals – last 24h"
    msg["From"] = Config.email_from
    msg["To"] = Config.email_to
    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(Config.smtp_host, Config.smtp_port, context=ctx) as s:
        s.login(Config.smtp_user, Config.smtp_pass)
        s.send_message(msg)
