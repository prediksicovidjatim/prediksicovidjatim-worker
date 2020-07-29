import sys
import main
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.jobstores.base import JobLookupError
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
    
    

DATABASE_URL = os.getenv("DATABASE_URL")
WORKER_NAME = os.getenv("WORKER_NAME")


#@sched.scheduled_job('interval', seconds=10)
def timed_job():
    print('This job is run every 10 seconds.')

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
        
        
jobstores = {
    'default': SQLAlchemyJobStore(
        url=DATABASE_URL, 
        tablename="apscheduler_" + WORKER_NAME
    )
}
sched = BlockingScheduler(jobstores=jobstores)

def try_remove_job(job_id):
    try:
        sched.remove_job(job_id)
    except JobLookupError as ex:
        pass
        
def clear_job():
    try_remove_job('timed_job')
    try_remove_job('daily_job')
    try_remove_job('weekly_job_1')
    try_remove_job('weekly_job_2')

def start_sched():
    
    #sched.add_job(timed_job, 'interval', seconds=10, misfire_grace_time=1, id='timed_job')
    
    next_run_time = datetime.now() if DEBUG else None
    
    if MODE == "daily":
        sched.add_job(daily_job, 'cron', hour=HOUR, minute=MINUTE, timezone=tz, max_instances=1, next_run_time=next_run_time, misfire_grace_time=600, id='daily_job')
    elif MODE == "fit_quick":
        sched.add_job(weekly_job_1, 'cron', day_of_week=DAY, hour=HOUR, minute=MINUTE, timezone=tz, max_instances=1, next_run_time=next_run_time, misfire_grace_time=600, id='weekly_job_1')
    elif MODE == "fit_test":
        sched.add_job(weekly_job_2, 'cron', day_of_week=DAY, hour=HOUR, minute=MINUTE, timezone=tz, max_instances=1, next_run_time=next_run_time, misfire_grace_time=600, id='weekly_job_2')
    else:
        raise Exception("Invalid mode: " + str(MODE))
        
    sched.start()
    
def restart_sched():
    clear_job()
    start_sched()
    
if __name__ == '__main__':
    start_sched()