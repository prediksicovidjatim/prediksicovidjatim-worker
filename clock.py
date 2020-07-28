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

MODE = os.getenv("WORKER_MODE")

DEBUG = os.getenv("WORKER_DEBUG")
if DEBUG:
    if DEBUG.lower() == "false":
        DEBUG = False
    else:
        try:
            DEBUG = int(DEBUG)
        except Exception:
            DEBUG = True

DAY = os.getenv("WORK_DAY")
HOUR = 1
try:
    HOUR = int(os.getenv("WORK_HOUR"))
except Exception:
    pass
MINUTE = 1
try:
    MINUTE = int(os.getenv("WORK_MINUTE"))
except Exception:
    pass

sched = BlockingScheduler()

#@sched.scheduled_job('interval', minutes=3)
def timed_job():
    print('This job is run every three minutes.')

#@sched.scheduled_job('cron', hour=19, minute=0, timezone=tz)
def daily_job():
    main.init()
    try:
        main.scrap()
    except Exception as ex:
        traceback.print_exc()
    try:
        main.map(any=True)
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
        main.map(any=True)
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
        main.map(any=True)
    except Exception as ex:
        traceback.print_exc()

def start_sched():
    next_run_time = datetime.now() if DEBUG else None
    #sched.add_job(timed_job, 'interval', minutes=3)
    if MODE == "daily":
        sched.add_job(daily_job, 'cron', hour=HOUR, minute=MINUTE, timezone=tz, max_instances=1, next_run_time=next_run_time )
    elif MODE == "fit_quick":
        sched.add_job(weekly_job_1, 'cron', day_of_week=DAY, hour=HOUR, minute=MINUTE, timezone=tz, max_instances=1, next_run_time=next_run_time)
    elif MODE == "fit_test":
        sched.add_job(weekly_job_2, 'cron', day_of_week=DAY, hour=HOUR, minute=MINUTE, timezone=tz, max_instances=1, next_run_time=next_run_time)
    else:
        raise Exception("Invalid mode: " + str(MODE))
    sched.start()
    
start_sched()