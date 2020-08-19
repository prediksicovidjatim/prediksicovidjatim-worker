import sys
import main
from prediksicovidjatim import database
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
    
def cron_job(hour, minute):
    print('this job is run at hour=%d minute=%d' % (hour, minute))

#@sched.scheduled_job('cron', hour=19, minute=0, timezone=tz)
def daily_job():
    main.init()
    try:
        main.scrap()
    except Exception as ex:
        traceback.print_exc()
    try:
        main.map(update_prediction=False, any=True)
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

'''
TABLE_NAME = "apscheduler_" + WORKER_NAME
jobstores = {
    'default': SQLAlchemyJobStore(
        url=DATABASE_URL, 
        tablename=TABLE_NAME
    )
}
sched = BlockingScheduler(jobstores=jobstores)
'''

sched = BlockingScheduler()

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
    database.init()
    with database.get_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM " + TABLE_NAME)
        conn.commit()

def start_sched():
    
    #sched.add_job(timed_job, 'interval', seconds=10, misfire_grace_time=1, id='timed_job')
    #print("Added timed_job")
        
    if MODE == "daily":
        sched.add_job(daily_job, 'cron', hour=HOUR, minute=MINUTE, timezone=tz, max_instances=1, misfire_grace_time=600, id='daily_job')
        print("Added daily_job")
        print("Job scheduled for run daily at %s:%s" % (str(HOUR), str(MINUTE)))
    elif MODE == "fit_quick":
        sched.add_job(weekly_job_1, 'cron', day_of_week=DAY, hour=HOUR, minute=MINUTE, timezone=tz, max_instances=1, misfire_grace_time=600, id='weekly_job_1')
        print("Added weekly_job_1")
        print("Job scheduled for run at %s, %s:%s" % (DAY, str(HOUR), str(MINUTE)))
    elif MODE == "fit_test":
        sched.add_job(weekly_job_2, 'cron', day_of_week=DAY, hour=HOUR, minute=MINUTE, timezone=tz, max_instances=1, misfire_grace_time=600, id='weekly_job_2')
        print("Added weekly_job_2")
        print("Job scheduled for run at %s, %s:%s" % (DAY, str(HOUR), str(MINUTE)))
    else:
        raise Exception("Invalid mode: " + str(MODE))
        
    sched.start()
    
def restart_sched():
    clear_job()
    start_sched()
    
if __name__ == '__main__':
    #restart_sched()
    start_sched()