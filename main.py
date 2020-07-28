import os
from dotenv import load_dotenv
load_dotenv()

from prediksicovidjatim import database, config
from prediksicovidjatim.data.model import ModelDataRepo
from prediksicovidjatim.util import Pool

K_TEST = os.getenv("K_TEST")

def init():
    database.init()
    #config.init_plot()
    with database.get_conn() as conn, conn.cursor() as cur:
        ModelDataRepo.init_weights(cur)
        
def scrap():
    from .core import scraping
    scraping.scrap_new_data()
        
def scrap_covid():
    from .core import scraping
    scraping.scrap_new_covid_data()
    
def scrap_hospital():
    from .core import scraping
    scraping.scrap_new_hospital_data()
    
def map():
    from .core import mapping
    mapping.init()
    mapping.update_map_all()
    
def fit(test=False):
    from .core import fitting
    if test:
        test_splits = [K_TEST]
    else:
        test_splits = []
    fitting.fit_all(test_splits, pool=fit_pool)
    