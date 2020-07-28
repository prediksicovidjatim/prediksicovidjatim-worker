import os
import sys
from memory_profiler import profile as mprofile
#sys.path.insert(0, "../../")
from prediksicovidjatim.data.map import MapDataRepo, MapDataPred, MapDataReal
from prediksicovidjatim import util, config, database
from prediksicovidjatim.modeling import SeicrdRlcModel
from prediksicovidjatim.mapping import MapUpdater
from prediksicovidjatim.util import ThreadPool, lprofile
from requests.exceptions import ConnectionError
from threading import RLock
import gc


from dotenv import load_dotenv
load_dotenv()
ARCGIS_USER = os.getenv("ARCGIS_USER")
ARCGIS_PASS = os.getenv("ARCGIS_PASS")
ARCGIS_PORTAL = os.getenv("ARCGIS_PORTAL")

REAL_LAYER_ID = os.getenv("REAL_LAYER_ID")
PRED_LAYER_ID = os.getenv("PRED_LAYER_ID")

PREDICT_DAYS = 30
try:
    PREDICT_DAYS = int(os.getenv("PREDICT_DAYS"))
except Exception:
    pass
    
FIRST_TANGGAL_STR = os.getenv("FIRST_TANGGAL") or '2020-03-20'
FIRST_TANGGAL = util.parse_date(FIRST_TANGGAL_STR)

def init(cur=None):
    global FIRST_TANGGAL
    FIRST_TANGGAL = MapDataRepo.get_oldest_tanggal(None, cur)

model_lock = RLock()
def predict(kabko, predict_days=PREDICT_DAYS):
    mod = SeicrdRlcModel(kabko)
    params = kabko.get_params_init(extra_days=predict_days)
    
    model_lock.acquire()
    model_result = mod.model(**params)
    model_lock.release()
    
    pred_data = MapDataPred.from_result(kabko, model_result)
    pred_data = MapDataPred.shift(pred_data, FIRST_TANGGAL)
    return pred_data

    
def get_updater(chunk_size=100):
    return MapUpdater(ARCGIS_PORTAL, ARCGIS_USER, ARCGIS_PASS, chunk_size=chunk_size)
    
def _update_map(updater, layer_id, selected_kabko, layer_data, update=True, chunk_size=100, max_process_count=None, max_tasks_per_child=10):
    layer = updater.get_layer(layer_id)
    return updater.save(layer, selected_kabko, layer_data, update=update, chunk_size=chunk_size, max_process_count=max_process_count, max_tasks_per_child=max_tasks_per_child)

def update_map(selected_kabko, chunk_size=100, tanggal=None, predict_days=PREDICT_DAYS, updater=None, db=None, max_process_count=None, max_tasks_per_child=2):
    print("Updating maps of %s" % (selected_kabko,))
    updater = updater or get_updater(chunk_size=chunk_size)
    
    db = db or database
    done = 0
    with db.get_conn() as conn, conn.cursor() as cur:
        real_data = MapDataRepo.fetch_real_data(selected_kabko, cur)
        kabko = MapDataRepo.get_kabko_full(selected_kabko, cur)
        
    real_data = MapDataReal.shift(real_data, FIRST_TANGGAL)
    done2, chunk_size2 = _update_map(updater, REAL_LAYER_ID, selected_kabko, real_data, update=False, chunk_size=chunk_size, max_process_count=max_process_count, max_tasks_per_child=max_tasks_per_child)
    #gc.collect()
    done += done2
    chunk_size = min(chunk_size, chunk_size2)
    
    if predict_days > 0:
        pred_data = predict(kabko, predict_days)
        done2, chunk_size2 = _update_map(updater, PRED_LAYER_ID, selected_kabko, pred_data, chunk_size=chunk_size, max_process_count=max_process_count, max_tasks_per_child=max_tasks_per_child)
        #gc.collect()
        done += done2
        chunk_size = min(chunk_size, chunk_size2)
    
    MapDataRepo.set_updated(selected_kabko, tanggal, chunk_size)
    print("Updated maps of %s" % (selected_kabko,))
    
    
    
def cache_geometry(updater=None, first_tanggal=FIRST_TANGGAL_STR):
    updater = updater or get_updater()
    layer = updater.get_layer(REAL_LAYER_ID)
    updater.cache_kabko_geometry(layer, util.format_date(first_tanggal))
    
def update_map_all(predict_days=PREDICT_DAYS, any=False, max_process_count=None, max_tasks_per_child=10, pool=None, inner_max_process_count=1, inner_max_tasks_per_child=100):
    latest_tanggal = None
    with database.get_conn() as conn, conn.cursor() as cur:
        #latest_tanggal = MapDataRepo.get_latest_tanggal(cur)
        kabko = MapDataRepo.fetch_kabko_need_mapping(latest_tanggal, any=any, cur)
        
    print("%s kabko needs updating" % (str(len(kabko)),))
            
    if len(kabko) == 0:
        print("No kabko to update maps")
        return
        
    print("Caching kabko geometry")
    cache_geometry()
    
    print("Updating maps of %s kabko" % (str(len(kabko)),))
    updater = None#get_updater()
    
    args = [(*k, latest_tanggal, predict_days, updater, database.singleton, inner_max_process_count, inner_max_tasks_per_child) for k in kabko]
    
    if not pool and max_process_count == 1:
        for arg in args:
            update_map(*arg)
    else:
        #gc.collect()
        #pool = pool or Pool(processes=max_process_count, maxtasksperchild=max_tasks_per_child)
        pool = pool or ThreadPool(processes=util.min_none(len(args), max_process_count))
        try:
            output = pool.starmap(update_map, args)
            pool.close()
            pool.join()
        except ConnectionError as ex:
            raise
        finally:
            pool.terminate()
            del pool