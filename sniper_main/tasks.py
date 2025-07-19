"""tasks.py – harmonogram z APScheduler.

• co Config.poll_interval_h – uruchom fetcher + ``daily_runner``
• raz dziennie o 02:00 UTC – ``aggregator`` + e-mail podsumowujący
"""

from apscheduler.schedulers.blocking import BlockingScheduler

from . import aggregator, daily_report, daily_runner
from .config import Config

sched = BlockingScheduler(timezone="UTC")


@sched.scheduled_job("interval", hours=Config.poll_interval_h)
def fetch_job() -> None:
    """Fetch new offers and process them."""
    daily_runner.main()


@sched.scheduled_job("cron", hour=2, minute=0)
def email_job() -> None:
    """Aggregate historic data and send daily summary email."""
    aggregator.aggregate()
    daily_report.send_daily_report()


if __name__ == "__main__":
    sched.start()
