import sys
import main
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import traceback
import os
from dotenv import load_dotenv
load_dotenv()

from pytz import timezone

tz = timezone("Asia/Jakarta")

mode = os.getenv("WORKER_MODE")
debug = os.getenv("WORKER_DEBUG")

sched = BlockingScheduler()


#@sched.scheduled_job('cron', hour=19, minute=0, timezone=tz)
def daily_job():
    main.init()
    try:
        main.scrap()
    except Exception as ex:
        traceback.print_exc()
    try:
        main.map(predict_days=0, any=True)
    except Exception as ex:
        traceback.print_exc()
    
    
#@sched.scheduled_job('cron', day_of_week='tue', hour=19, minute=30, timezone=tz)
def weekly_job_1():
    main.init()
    try:
        main.fit(False)
    except Exception as ex:
        traceback.print_exc()
    try:
        main.map()
    except Exception as ex:
        traceback.print_exc()
    
#@sched.scheduled_job('cron', day_of_week='sat', hour=19, minute=30, timezone=tz)
def weekly_job_2():
    main.init()
    try:
        main.fit(True)
    except Exception as ex:
        traceback.print_exc()
    try:
        main.map()
    except Exception as ex:
        traceback.print_exc()

def start_sched():
    next_run_time = datetime.now() if debug else None
    if mode == "daily":
        sched.add_job(daily_job, 'cron', hour=0, minute=10, timezone=tz, max_instances=1, next_run_time=next_run_time )
    elif mode == "fit_quick":
        sched.add_job(weekly_job_1, 'cron', day_of_week='wed', hour=1, minute=0, timezone=tz, max_instances=1, next_run_time=next_run_time)
    elif mode == "fit_test":
        sched.add_job(weekly_job_2, 'cron', day_of_week='sun', hour=1, minute=0, timezone=tz, max_instances=1, next_run_time=next_run_time)
    else:
        raise Exception("Invalid mode: " + str(mode))
    sched.start()
    
start_sched()