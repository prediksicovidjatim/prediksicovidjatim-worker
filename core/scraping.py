import os
import sys
#sys.path.insert(0, "../../")

import traceback

from prediksicovidjatim import database, util
from prediksicovidjatim.data.raw import Scrapper, RawDataRepo
from prediksicovidjatim.data.raw.entities import RawData
from prediksicovidjatim.data.kapasitas_rs import KapasitasRSScrapper, KapasitasRSRepo

    
def _scrap_new_covid_data(scrapper, kabko, tanggal, max_process_count=6):
    tanggal = util.filter_dates_after(tanggal, RawDataRepo.get_latest_tanggal())
    l = len(tanggal)
    if l == 0:
        print("No new data")
        return 0
    print("Filling %d days worth of data, from %s to %s." % (l, tanggal[0], tanggal[-1]))
    for t in tanggal:
        data = scrapper.scrap_bulk(kabko, [t], max_process_count)
        RawDataRepo.save_data([d.to_db_row() for d in data])
        print("Done scraping: " + t)
    print("Done scraping %s days of data" % (l,))
    return len(tanggal)
        
def scrap_new_covid_data():
    print ("Scraping new covid data")
    scrapper = Scrapper()
    params = None
    kabko = RawDataRepo.fetch_kabko()
    counter = 0
    for i in range(0, 5):
        try:
            params = params or scrapper.scrap_params()
            return _scrap_new_covid_data(scrapper, kabko, params.tanggal)
        except ConnectionError as ex:
            if counter >= 4:
                #traceback.print_exc()
                raise
            counter += 1
        
def _scrap_new_hospital_data(scrapper):
    data = scrapper.scrap()
    if len(data) == 0:
        print("No new hospital data")
        return 0
    KapasitasRSRepo.save(data)
    KapasitasRSRepo.fix_kapasitas()
    print ("Got %s possibly new hospital data" % (str(len(data)),))
    return len(data)
    
def scrap_new_hospital_data():
    print ("Scraping new hospital data")
    scrapper = KapasitasRSScrapper()
    counter = 0
    while True:
        try:
            return _scrap_new_hospital_data(scrapper)
        except ConnectionError as ex:
            if counter >= 4:
                #traceback.print_exc()
                raise
            counter += 1
            
def scrap_new_data():
    print ("Scraping new data")
    need_update = 0
    try:
        need_update += scrap_new_covid_data()
    except ConnectionError:
        raise
    finally:
        #need_update += 
        scrap_new_hospital_data()
        return need_update
    
