# -*- coding: utf-8 -*-
from datetime import datetime,timedelta
from lib import connect_DB,cwb_DTR,load_grid_data,check_miss_time,go_to_log
import ref

#######################################
#待新增修改：
# try except log
# 若新增節點,沒有這個資料庫的話,新增此資料庫
# 修改dtr公式,加入風向與線段夾角的判斷(目前預設夾角90度)
# 補齊註解
# 將grid_id 存入資料庫避免浪費運算資源
#######################################


now = datetime.now()+timedelta(hours=1)
grid_lat,grid_lon=load_grid_data()
# 正常傳資料(架空)
segids = connect_DB(ref.db_info,"Gridwell",ref.sql,'select',0)
for segid in segids:
    cwb_DTR(now,segid[0],grid_lat,grid_lon,ref.log_path,"Gridwell")

# 正常傳資料(塔基)
segids_towerbase = connect_DB(ref.db_info,"TowerBase_Gridwell",ref.sql,'select',0)
for segid_tb in segids_towerbase:
    cwb_DTR(now,segid_tb[0],grid_lat,grid_lon,ref.log_path,"TowerBase_Gridwell")


# 半夜三點補傳資料
if now.hour == 3:
    #補傳架空資料
    for segid in segids:
        result = check_miss_time(ref.db_info,ref.insert_db_name,segid[0],24)
        if len(result) != 0:
            for i in result:
                cwb_DTR(i+timedelta(hours=1),segid[0],grid_lat,grid_lon,ref.log_path,"Gridwell")
    #補傳塔基資料
    for segid_tb in segids_towerbase:
        result = check_miss_time(ref.db_info,ref.insert_db_name,segid_tb[0],24)
        if len(result) != 0:
            for i in result:
                cwb_DTR(i+timedelta(hours=1),segid_tb[0],grid_lat,grid_lon,ref.log_path,"TowerBase_Gridwell")

go_to_log(ref.log_path,'all data update')
