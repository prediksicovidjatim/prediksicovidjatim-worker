import os
from dotenv import load_dotenv
load_dotenv()

from prediksicovidjatim import database, config
from prediksicovidjatim.data.model import ModelDataRepo
from prediksicovidjatim.util import Pool

K_TEST = 3
try:
    K_TEST = int(os.getenv("K_TEST"))
except Exception:
    pass

PREDICT_DAYS = 30
try:
    PREDICT_DAYS = int(os.getenv("PREDICT_DAYS"))
except Exception:
    pass

def init():
    database.init()
    #config.init_plot()
    with database.get_conn() as conn, conn.cursor() as cur:
        ModelDataRepo.init_weights(cur)
        
def scrap():
    from core import scraping
    scraping.scrap_new_data()
        
def scrap_covid():
    from core import scraping
    scraping.scrap_new_covid_data()
    
def scrap_hospital():
    from core import scraping
    scraping.scrap_new_hospital_data()
    
def map(any=False, predict_days=PREDICT_DAYS, update_prediction=True, update_real=False):
    from core import mapping
    mapping.init()
    mapping.update_map_all(any=any, predict_days=predict_days, update_prediction=update_prediction, update_real=update_real)
    
def fit(test=False):
    from core import fitting
    if test:
        test_splits = [K_TEST]
    else:
        test_splits = []
    fitting.fit_all(test_splits)
    