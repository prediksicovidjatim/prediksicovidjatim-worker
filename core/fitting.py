import os
import sys
#sys.path.insert(0, "../../")


import warnings
warnings.filterwarnings('ignore')

from prediksicovidjatim import util, config, database
from prediksicovidjatim.data.model import ModelDataRepo
from prediksicovidjatim.data.model.entities import *
from prediksicovidjatim.modeling import SeicrdRlcModel
from prediksicovidjatim.util import Pool, lprofile
from memory_profiler import profile as mprofile

import gc
from threading import RLock

fit_lock = RLock()

@lprofile
def fit(selected_kabko, test_splits=[], tanggal=None, kabko=None, db=None):
    print("Fitting %s with %s test_splits" % (selected_kabko, str(test_splits)))
    #sys.stdout.flush()
    db = db or database
    if not kabko:
        with db.get_conn() as conn, conn.cursor() as cur:
            kabko = ModelDataRepo.get_kabko_full(selected_kabko, cur=cur)
    mod = SeicrdRlcModel(kabko)
    
    fit_lock.acquire()
    result = mod.fit(method="leastsq", test_splits=test_splits)
    fit_lock.release()
    
    with db.get_conn() as conn, conn.cursor() as cur:
        ModelDataRepo.save_fitting_result(result, tanggal, cur=cur)
    print("Done fitting %s with %s test_splits" % (selected_kabko, str(test_splits)))
    #sys.stdout.flush()
    
@lprofile
def fit_all(test_splits=[], kabko=None, max_process_count=1, max_tasks_per_child=None, pool=None):
    with database.get_conn() as conn, conn.cursor() as cur:
        latest_tanggal = ModelDataRepo.get_latest_tanggal(cur)
        if not kabko:
            kabko = ModelDataRepo.fetch_kabko_need_fitting(latest_tanggal, cur)
            
    print("Fitting %s kabko with %s test_splits and %s process" % (str(len(kabko)), str(test_splits), str(max_process_count)))
    
    args = [(k, test_splits, latest_tanggal, None, database.singleton) for k in kabko]
    
    if not pool and max_process_count == 1:
        for arg in args:
            fit(*arg)
    else:
        gc.collect()
        pool = pool or Pool(processes=max_process_count, maxtasksperchild=max_tasks_per_child)
        #pool = pool or ThreadPool(processes=util.min_none(len(args), max_process_count))
        try:
            output = pool.starmap(fit, args)
            pool.close()
            pool.join()
        except Exception as ex:
            raise
        finally:
            pool.terminate()
            del pool
            gc.collect()
        
    