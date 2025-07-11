"""
tasks.py – prosty harmonogram APScheduler.
• co 6 h: fetch + daily_runner
• raz dziennie 02:00 UTC: agregator + wysyłka e-maila
"""
from apscheduler.schedulers.blocking import BlockingScheduler
import daily_runner, aggregator

sched = BlockingScheduler(timezone="UTC")

@sched.scheduled_job("interval", hours=6)
def fetch_job():
    daily_runner.main()

@sched.scheduled_job("cron", hour=2, minute=0)
def email_job():
    aggregator.aggregate()
    aggregator.send_daily_report()

if __name__ == "__main__":
    sched.start()
