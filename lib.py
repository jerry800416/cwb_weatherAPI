# -*- coding: utf-8 -*-
import MySQLdb
from datetime import datetime,timedelta
from DTR_161 import Solve_I
import time
import ref
import os
import pandas as pd
import numpy as np
import math


class find_closest_grid():
    '''
    藉由lat,lon返回氣象局的天氣資訊
    '''
    def __init__(self,path,time,lat,lon,grid_lat,grid_lon,log_path):
        self.path = path
        self.scandate = time + timedelta(hours=-1)
        self.filedate = self.scandate + timedelta(hours=-8)
        self.lat = lat
        self.lon = lon
        self.grid_lat = grid_lat
        self.grid_lon = grid_lon
        self.log_path = log_path


    def find_closest (self):
    	# 找最近格點
        min=1000000
        # grid_lat,grid_lon=load_grid_data()
        for i in range(0,67600):
            g_lat=self.grid_lat[i]
            g_lon=self.grid_lon[i]
            d=find_closest_grid.distance(self,[self.lat,self.lon],[g_lat,g_lon])
            if (d<min):
                min=d
                min_id=i
        return min_id


    def distance (self,pole1,pole2):
    	# 兩點距離
        d=math.sqrt((pole1[0]-pole2[0])**2+(pole1[1]-pole2[1])**2)
        return d


    def update_data_with_type(self,datatype):
        try:
            # 找最近格點編號
            grid_id = find_closest_grid.find_closest(self)
            # 檔案名稱,資料屬性，溫度是temp，風速是ws
            filename = 'CWBgt_'+str(self.filedate.year)+'%02d' % self.filedate.month + \
                '%02d' % self.filedate.day+'%02d' % self.filedate.hour+'00_000_'+datatype.upper()+'.dat'
            # 讀氣象資料260*260
            grid_data = 0
            CWB_data = np.loadtxt(self.path+filename)
            # 讀目標格點之資料
            if CWB_data.shape == (260, 260):
                grid_data = CWB_data[grid_id//260, grid_id % 260]
            else:
                grid_data = -1
            # 放進時間資料
            if datatype == 'temp':
                if grid_data > 200:
                    grid_data -= 273.15
        except Exception as e:
            go_to_log(self.log_path,e)
            grid_data = -1
        return grid_data


def load_grid_data():
    '''
    返回計算的網格資料
    '''
    path = os.path.abspath(os.path.dirname(__file__))   
    filename = path + '/grid_info.xlsx'
    wb= pd.read_excel(filename)
    grid_lat=[]
    grid_lon=[]
    for i in range(len(wb)):
        grid_lat.append(wb.iloc[i,0])
        grid_lon.append(wb.iloc[i,1])
    return grid_lat,grid_lon


def go_to_log(log_path,e):
    '''
    '''
    with open(log_path,'a', newline='') as f:
        f.write('{} :{}\n'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),e))


def connect_DB(db_info, dbname, sql, sql_type, fetch):
    '''
    select 和 insert 資料庫操作\n
    db_info: secret\n
    db_name: 要操作的db名稱\n
    sql: sql語法\n
    sql_type: chose select or insert\n
    fetch:fetch all or fetch one
    '''
    conn = MySQLdb.connect(host=db_info[0],user=db_info[1],passwd=db_info[2],port=db_info[3],db=dbname)
    cur = conn.cursor()
    cur.execute(sql)
    if sql_type == 'select':
        if fetch == 0:
            result = cur.fetchall()
        else:
            result = cur.fetchone()
        cur.close()
        conn.commit()
        conn.close()
        return result
    elif sql_type in ('insert','delete','update'):
        cur.close()
        conn.commit()
        conn.close()


def catch_tower_data(segid,db_info,dbname,log_path):
    '''
    '''
    # 拉取電塔座標與海拔
    sql = "SELECT SegID,Latitude,Longitude,Altitude,RouteID,TowerOrder FROM `Segment` WHERE SegID = '{}'".format(segid)
    result = connect_DB(db_info,dbname,sql,'select',0)
    # 拉取鄰近電塔座標
    neighbor_tower_sql = "SELECT Latitude,Longitude FROM `Segment` WHERE RouteID = {} AND TowerOrder > {}".format(result[0][4],result[0][5])
    neighbor_tower = connect_DB(db_info,dbname,neighbor_tower_sql,'select',1)
    # 如果是最後一座電塔,則拉取上一座電塔
    # 如果是最後一座電塔,則拉取上一座電塔
    if neighbor_tower == None:
        neighbor_tower_sql = "SELECT Latitude,Longitude FROM `Segment` WHERE RouteID = {} AND TowerOrder < {}".format(result[0][4],result[0][5])
        neighbor_tower = connect_DB(db_info,dbname,neighbor_tower_sql,'select',1)
    # 拉取線徑
    sql = "SELECT diameter FROM `STR` WHERE CableType = (SELECT CableType FROM `RouteInfo` WHERE RouteID = {})".format(result[0][4])
    diameter = connect_DB(db_info,dbname,sql,'select',1)
    results = {"segid":result[0][0],"lat":result[0][1],"lon":result[0][2],"Alt":result[0][2],"Nlat":neighbor_tower[0],"Nlon":neighbor_tower[1],"cab":diameter[0]}
    return results
    

def check_miss_time(db_info,dbname,tablename,timerange):
    '''
    檢查資料庫裡面某個時間區段是否有漏傳資料,若有,則返回漏傳的時間list\n
    timerange: 要檢查幾個小時前到目前的資料(type:int)\n
    dbname : 要檢查的db(type:str)
    '''
    result_list = []
    miss_list =[]
    st_time = (datetime.now()- timedelta(hours=timerange)).strftime("%Y-%m-%d %H:00:00")
    if tablename == "predict_DTR":
        sql = "SELECT DISTINCT {0} FROM `{1}` WHERE {0} > '{2}' ORDER BY {0} ASC".format("Cur_time",tablename,st_time)
    else :
        sql = "SELECT DISTINCT {0} FROM `{1}` WHERE {0} > '{2}' ORDER BY {0} ASC".format("time",tablename,st_time)
    result = list(connect_DB(db_info,dbname,sql,'select',0))
    if len(result) != 0:
        for i in result:
            result_list.append(i[0])
        for i in range(len(result_list)):
            if i <= (len(result_list)-2):
                while result_list[i+1] != result_list[i]+timedelta(hours=1):
                    result_list[i] += timedelta(hours=1)
                    miss_list.append(result_list[i])
    return miss_list


def check_time(db_info,dbname,tablename,time):
    '''
    檢查資料庫是否有最新資料
    '''
    sql = "SELECT time FROM `{}` ORDER BY time DESC LIMIT 1".format(tablename)
    result = connect_DB(db_info,dbname,sql,'select',1)[0].strftime("%Y-%m-%d %H:00:00")
    if result == time.strftime("%Y-%m-%d %H:00:00"):
        return False
    else :
        return True



def cwb_DTR(time,segid,grid_lat,grid_lon,log_path,dbname):
    '''
    '''
    # 拉取計算dtr所需要的參數
    tower_info = catch_tower_data(segid,ref.db_info,dbname,log_path)
    He = tower_info["Alt"] #海拔
    D0 = tower_info["cab"] #線徑
    p1 = [tower_info["lat"],tower_info["lon"]] #電塔座標
    p2 = [tower_info["Nlat"],tower_info["Nlon"]] #鄰近電塔座標
    Tc = 80  # 線溫上限
    scandate = time + timedelta(hours =-1)
    day = scandate.strftime("%m/%d/%Y")
    h = scandate.hour
    # 拉取氣象資料 ws,wd,temp,rh
    fcg = find_closest_grid(ref.data_path,time,p1[0],p1[1],grid_lat,grid_lon,ref.log_path)
    node_ws = fcg.update_data_with_type("ws")
    node_temp = fcg.update_data_with_type("temp")
    node_wd = fcg.update_data_with_type("wd")
    node_rh = fcg.update_data_with_type("rh")
    # 若無值迴圈拉取上一筆資料
    lttime = time 
    while -1 in [node_ws,node_temp,node_wd]:
        lttime += timedelta(hours =-1)
        fcg = find_closest_grid(ref.data_path,lttime,p1[0],p1[1],grid_lat,grid_lon,ref.log_path)
        node_ws = fcg.update_data_with_type("ws")
        node_temp = fcg.update_data_with_type("temp")
        node_wd = fcg.update_data_with_type("wd")
        node_rh = fcg.update_data_with_type("rh")
        node_rain = fcg.update_data_with_type("rain")

    # 計算dtr
    if -1 not in [node_ws,node_temp]:
        DTR = round(Solve_I(Tc,node_temp,He,node_ws,day,h,p1,p2,D0),2)
    else:
        DTR = -1
    # insert to database
    if dbname == "TowerBase_Gridwell":
        sql = "INSERT INTO `{}`(time,WS,WD,temp,DTR,RH,rainfall) VALUES ('{}',{},{},{},{},{},{})".format(segid,scandate.strftime("%Y-%m-%d %H:00:00"),node_ws,node_wd,node_temp,DTR,node_rh,node_rain)
    else:
        sql = "INSERT INTO `{}`(time,WS,WD,temp,DTR,RH,rainfall) VALUES ('{}',{},{},{},{},{},{})".format(segid,scandate.strftime("%Y-%m-%d %H:00:00"),node_ws,node_wd,node_temp,DTR,node_rh,node_rain)
    # TODO
    connect_DB(ref.db_info,ref.insert_db_name,sql,'insert',0)
    # connect_DB(ref.db_info2,ref.insert_db_name,sql,'insert',0)

    