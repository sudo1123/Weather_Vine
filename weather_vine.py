# -*- coding: utf-8 -*-
# WeatherVine - 天气数据采集系统
# Copyright (C) 2025 屈祺QU QI
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
import requests
import time
from DB_Manager_SM import Database_Manager
import yaml
# 加载配置文件
with open('config.yaml',"r",encoding='utf-8') as f:
    config = yaml.safe_load(f)
db_config = config['database']

DB_object = Database_Manager(
    host=db_config['host'],
    user=db_config['user'],
    password=db_config['password'],
    database=db_config['name'],
    port=db_config['port']
)
api=config["api"]
Amap_api=api["key"]
City_name=api["city"]
Record_update_interval=config["update"]["update_interval"]
#设置全局变量
last_update=0.0

class SearchWeather:
    '''
    key:高德地图API密钥
    '''
    def __init__(self,key):
        self.key=key
        self.all_weather_data={}
    def get_data(self,adcode):
        #api_address:高德地图实时天气api的获取接口
        self.api_address=f"https://restapi.amap.com/v3/weather/weatherInfo?key={self.key}&city={adcode}&extensions=base"
        self.response = requests.get(self.api_address)
        if self.response.status_code == 200:  #请求成功
            self.data = self.response.json()  #解析内容为json格式
            return self.data
        else: #请求失败
            print("请求失败，状态码：", self.response.status_code)
            return
    def get_all_districts_adcode(self,city_name):
        self.url = f"https://restapi.amap.com/v3/config/district?keywords={city_name}&key={self.key}"
        response = requests.get(self.url)
        if response.status_code == 200:
            data = response.json()
            districts = data['districts'][0]['districts']  # 获取所有区
            self.districts = [district['adcode'] for district in districts]
            return self.districts#返回包含所有区的adcode的列表
        else:
            print("请求失败，状态码：", response.status_code)
            self.districts=[]
            return self.districts#返回空列表
        
    def get_and_address_data(self,districts=None):
        '''循环查询所有区的天气数据'''
        if districts==None:
            districts=self.districts
        self.all_weather_data={}
        for district in districts:
            response=self.get_data(district)#获取天气数据（dict类型）
            raw_weather_data=response["lives"]#提取字典中对应键名为“live”的列表
            weather_data=raw_weather_data[0]#提取列表的第一项中的字典
            self.all_weather_data[str(time.time())]=weather_data#将区级数据保存进字典并使用保存时的时间戳为键名
            time.sleep(0.3)#设置请求间隔（防止api触发反爬）
            
        self.weather_data_keys=list(weather_data.keys())#获取字典中的所有键名并保存为列表
        self.create_time_stamps=list(self.all_weather_data.keys())#获取所有作为键名的时间戳并保存为列表

class WeatherDB_Manager():
    '''
    weather_data_keys:包含所有天气数据键名的列表
    create_time_stamps:包含所有创建时间的列表
    all_weather_data:包含所有天气数据（连同创建时间）的字典
    '''
    def __init__ (self,weather_data_keys,create_time_stamps,all_weather_data):
        self.weather_data_keys=weather_data_keys
        self.create_time_stamps=create_time_stamps
        self.all_weather_data=all_weather_data
        
    def insert_new_record(self,primary_key,weather_data_keys=None):#插入一条除主键外全null的记录
        if weather_data_keys==None:
            weather_data_keys=self.weather_data_keys
        '''拼接构建sql插入语句'''
        insert_string_command=str(primary_key)
        for index in range(len(weather_data_keys)+1):#加一的原因是添加了时间戳列，导致实际列数会多一列
            insert_string_command+=",null"
        '''插入一条除主键外全null的记录'''
        DB_object.insert_string(insert_string_command)
        
    def create_new_primary_key(self):#创建新主键(比原最大主键多一)
        latest_primary_key=DB_object.get_max_primary_key()#获取最大主键
        if latest_primary_key=="NPK":#表为空表，没有主键值
            self.new_primary_key=1
            return 1
        else:
            self.new_primary_key=latest_primary_key+1#创建比原主键加一的新主键
            return self.new_primary_key
        
    def update_all_fields(self,timestamp,primary_key=None,weather_data_keys_local=None):#更新除去主键外所有列的值
        if primary_key==None:
            primary_key=self.new_primary_key
        if weather_data_keys_local==None:
            weather_data_keys_local=self.weather_data_keys
        for titled in weather_data_keys_local:#构建sql更新指令
                DB_object.update_string(titled,primary_key,self.all_weather_data[timestamp][titled])
        DB_object.update_string("create_time",primary_key,timestamp)        
print("欢迎使用Weather Vine,请稍等...")
search=SearchWeather(Amap_api)#初始化api查询对象  
search.get_all_districts_adcode(City_name)#获取目标城市下所有区级行政区adcode

'''
利用adcode,查询并处理所有区的天气数据。获得:
    键名为创建记录时间、键值为包含单行政区所有数据的字典的总字典(保存在all_weather_data属性)
    包含所有创建时间的列表(保存在create_time_stamps属性)
    包含所有天气数据键名的列表(保存在weather_data_keys属性)
'''  
search.get_and_address_data()
     
def update_data():#更新数据函数
    search.get_and_address_data()
    manager=WeatherDB_Manager(search.weather_data_keys,search.create_time_stamps,search.all_weather_data)#创建数据库交互对象
    global last_update
    for timestamp in search.create_time_stamps:
        manager.insert_new_record(manager.create_new_primary_key())#插入新主键记录
        manager.update_all_fields(timestamp) #更新后续所有列

DB_object.connect()
DB_object.create_cursor()
DB_object.set_default_table("weather_data")
if DB_object.exist_table() :
    choice=input("当前数据库中已有weather_data表，是否删除旧表并建立新表？（y为是，n为否）")
    if choice == "y":
        DB_object.drop_table()#删除旧表
if DB_object.exist_table() :
    pass
else:
    create_table_command=""
    for titled in search.weather_data_keys:
        create_table_command+=f"{titled} TEXT,"#构建sql建表指令(根据获取到的天气数据标题建立对应列)
    create_table_command+="create_time TEXT"#添加创建时间列
    DB_object.create_table(create_table_command)
    
while True:
    if time.time()-last_update >=Record_update_interval:#是否到达定时
        update_data()
        print("已更新数据")
        last_update=time.time()#更新上一次更新时间