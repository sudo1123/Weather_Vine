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
#DB_Manager_SM第三版
import mysql.connector
#定义Database_Manager类
class Database_Manager:
    '''定义对象属性'''
    def __init__(self,host,user,password,database,port):
        self.host=host          # 数据库服务器地址
        self.user=user          # 用户名
        self.password=password  # 密码
        self.database=database  # 数据库名
        self.port=port          # 端口号
        self.status=False       # 工作状态
        
    '''设定默认表，方法缺失表名参数时自动调用默认表'''
    def set_default_table(self,table_name):
        self.table_name=table_name
        
    '''连接函数，连接成功则设置self.status属性为True,失败则打印异常信息并返回'''
    def connect(self):
        try:
            self.conn=mysql.connector.connect(
            host=self.host,             # 数据库服务器地址
            user=self.user,             # 用户名
            password=self.password,     # 密码
            database=self.database,     # 数据库名
            port=self.port              # 端口号
            )
            self.status=True
        except mysql.connector.Error as err:  # 捕获MySQL特定的错误
            print("MySQL连接失败,错误信息",err)
            return 
        except Exception as e:
            print("未知错误，错误信息:",e)
            return
        
    '''关闭游标'''         
    def close_cursor(self):
        try:
            self.cursor.close()
        except AttributeError:
            print("错误：要关闭的游标不存在") 
        except Exception as error:
            print("发生错误，错误信息：",error)
    '''创建游标(单向)'''
    def create_cursor(self):
        self.cursor = self.conn.cursor()
        
    '''检查当前数据库是否存在给定表。
       table_name参数即为待查表名（缺失则为默认表）。如有则返回True,没有则返回False'''
    def exist_table(self,table_name=None):#检查是否存在表函数
        if table_name==None:
            table_name=self.table_name
        self.cursor.execute("SHOW TABLES")
        search_result=self.cursor.fetchone()
        if search_result==None:
            return False
        else:
            for index in range(len(search_result)):
                if search_result[index]==table_name:
                    return True
            return False
    '''向表中插入一条新记录，content参数即为待插入记录（如“1,2,3”）'''
    def insert_string(self,content):#向表中增加新记录函数
        sql=f"INSERT INTO {self.table_name} VALUES({content})"
        self.cursor.execute(sql)
        self.conn.commit()
        
    '''更新表中记录（依赖于主键（自增整数列）定位），
       new_value为更新值，column为待更新值所在列，
       primary_key为待更新值所在行的主键'''
    def update_string(self,column,primary_key,new_value,table_name=None):#更新表中记录函数
        if table_name==None:
            table_name=self.table_name   
        sql=f"UPDATE {table_name} SET {column} =%s WHERE primary_key={str(primary_key)}"
        self.cursor.execute(sql,(new_value,))
        self.conn.commit()
    
    '''遍历全表数据，逐列获取数据，table_name参数为待查询的表名（缺失则为默认表名）'''
    def traverse_database_data(self,table_name=None):#遍历并打印数据表数据函数
        if table_name==None:
            table_name=self.table_name     
        sql=f"SELECT * FROM {table_name}"
        self.cursor.execute(sql)
        output=self.cursor.fetchall()
        for row in range(len(output)):
            print(str(output[row])+"\n")
    '''创建表'''  
    def create_table(self,table_content,table_name=None):#创建表函数(强制带有主键列)
        self.cursor.execute("USE"+" "+self.database)
        if table_name==None:
            table_name=self.table_name
        content=table_content
        sql=f"CREATE TABLE {table_name}(primary_key INT AUTO_INCREMENT PRIMARY KEY,{content})"
        self.cursor.execute(sql)
    
    '''删除表'''
    def drop_table(self,table_name=None):#删除function_menu表函数
        self.cursor.execute("USE"+" "+self.database)
        if table_name==None:
            table_name=self.table_name
        sql=f"DROP TABLE {table_name}"
        self.cursor.execute(sql)
        
    '''获取表中特定的一列数据（column参数为待查列名）'''
    def get_single_column(self,column,table_name=None):
        result=[]
        if table_name==None:
            table_name=self.table_name
        sql=f"SELECT {column} FROM {table_name}"
        self.cursor.execute(sql)
        output=self.cursor.fetchall()
        return output 
    def get_max_primary_key(self,table_name=None):
        if table_name==None:
            table_name=self.table_name
        self.cursor.execute(f"SELECT MAX(primary_key) FROM {table_name}")
        result=self.cursor.fetchone()
        if result==(None,):
            return "NPK"  #"NO PRIMARY KEY"
        else:
            return result[0]