import main
from apscheduler.schedulers.blocking import BlockingScheduler
import datetime
import traceback

from pytz import timezone

tz = timezone("Asia/Jakarta")

sched = BlockingScheduler()


@sched.scheduled_job('cron', hour=19, minute=0, timezone=tz)
def daily_job():
    try:
        main.scrap_hospital()
    except Exception as ex:
        traceback.print_exc()
    
@sched.scheduled_job('cron', day_of_week='wed,fri,sun', hour=19, minute=30, timezone=tz)
def three_day_job():
    try:
        main.scrap_covid()
    except Exception as ex:
        traceback.print_exc()
    
@sched.scheduled_job('cron', day_of_week='sat', hour=19, minute=30, timezone=tz)
def weekly_job_1():
    try:
        main.fit(False)
    except Exception as ex:
        traceback.print_exc()
    try:
        main.map()
    except Exception as ex:
        traceback.print_exc()
    
@sched.scheduled_job('cron', day_of_week='tue', hour=19, minute=30, timezone=tz)
def weekly_job_2():
    try:
        main.fit(True)
    except Exception as ex:
        traceback.print_exc()
    try:
        main.map()
    except Exception as ex:
        traceback.print_exc()

sched.start()