# -*- coding: utf-8 -*-
from datetime import datetime,timedelta
from lib import connect_DB,cwb_DTR,load_grid_data,check_miss_time,go_to_log
import ref

now = datetime.now()+timedelta(hours=1)
grid_lat,grid_lon=load_grid_data()

# 正常傳資料(塔基)
# segids_towerbase = connect_DB(ref.db_info,"TowerBase_Gridwell",ref.sql,'select',0)
segids = connect_DB(ref.db_info,"Gridwell",ref.sql,'select',0)

#補傳塔基資料
for segid_tb in segids:
    result = check_miss_time(ref.db_info,ref.insert_db_name,segid_tb[0],999)
    if len(result) != 0:
        for i in result:
            cwb_DTR(i+timedelta(hours=1),segid_tb[0],grid_lat,grid_lon,ref.log_path,"TowerBase_Gridwell")
            pass