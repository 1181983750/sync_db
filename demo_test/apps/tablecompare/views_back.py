import datetime
import json
import math
import multiprocessing
import re
import time
import traceback
from pprint import pprint
from typing import Union, Dict, List

import pymssql
import requests

from django.http import HttpResponse, JsonResponse
from django.views.decorators.clickjacking import xframe_options_exempt
from django.views.decorators.http import require_http_methods

from .util import sqlserver, datahandle

source_server = {"ip": "172.17.18.110", "port": 1433, "user": "sa", "password": "CgSqlServerRoot2012", "db": "cgyypt"}
target_server = {"ip": "172.17.18.110", "port": 1433, "user": "sa", "password": "CgSqlServerRoot2012", "db": "cgyypt_2"}
jiaoben = """CREATE PROC p_helpindex
    (
		@tbname sysname = '' ,
    @CLUSTERED INT = '1'
		)
AS 


    IF @tbname IS NULL
        OR @tbname = ''
        RETURN -1;



    DECLARE @t TABLE
        (
          table_name NVARCHAR(100) ,
          schema_name NVARCHAR(100) ,
          fill_factor INT ,
          is_padded INT ,
          ix_name NVARCHAR(100) ,
          type INT ,
          keyno INT ,
          column_name NVARCHAR(200) ,
          cluster VARCHAR(20) ,
          ignore_dupkey VARCHAR(20) ,
          [unique] VARCHAR(20) ,
          groupfile VARCHAR(10)
        );

    DECLARE @table_name NVARCHAR(100) ,
        @schema_name NVARCHAR(100) ,
        @fill_factor INT ,
        @is_padded INT ,
        @ix_name NVARCHAR(100) ,
        @ix_name_old NVARCHAR(100) ,
        @type INT ,
        @keyno INT ,
        @column_name NVARCHAR(100) ,
        @cluster VARCHAR(20) ,
        @ignore_dupkey VARCHAR(20) ,
        @unique VARCHAR(20) ,
        @groupfile VARCHAR(10);

    DECLARE ms_crs_ind CURSOR LOCAL STATIC
    FOR
        SELECT

DISTINCT        table_name = a.name ,
                schema_name = b.name ,
                fill_factor = c.OrigFillFactor ,
                is_padded = CASE WHEN c.status = 256 THEN 1
                                 ELSE 0
                            END ,
                ix_name = c.name ,
                type = c.indid ,
                d.keyno ,
                column_name = e.name
                + CASE WHEN INDEXKEY_PROPERTY(a.id, c.indid, d.keyno,
                                              'isdescending') = 1
                       THEN ' desc '
                       ELSE ''
                  END ,
                CASE WHEN ( c.status & 16 ) <> 0 THEN 'clustered'
                     ELSE 'nonclustered'
                END ,
                CASE WHEN ( c.status & 1 ) <> 0 THEN 'IGNORE_DUP_KEY'
                     ELSE ''
                END ,
                CASE WHEN ( c.status & 2 ) <> 0 THEN 'unique'
                     ELSE ''
                END ,
                g.groupname
        FROM    sysobjects a
                INNER JOIN sysusers b ON a.uid = b.uid
                INNER JOIN sysindexes c ON a.id = c.id
                INNER JOIN sysindexkeys d ON a.id = d.id
                                             AND c.indid = d.indid
                INNER JOIN syscolumns e ON a.id = e.id
                                           AND d.colid = e.colid
                INNER JOIN sysfilegroups g ON g.groupid = c.groupid
                LEFT JOIN master.dbo.spt_values f ON f.number = c.status
                                                     AND f.type = 'I'
        WHERE   a.id = OBJECT_ID(@tbname)
                AND c.indid < 255
                AND ( c.status & 64 ) = 0
                AND c.indid >= @CLUSTERED
        ORDER BY c.indid ,
                d.keyno;


    OPEN ms_crs_ind;

    FETCH ms_crs_ind INTO @table_name, @schema_name, @fill_factor, @is_padded,
        @ix_name, @type, @keyno, @column_name, @cluster, @ignore_dupkey,
        @unique, @groupfile;


    IF @@fetch_status < 0
        BEGIN

            DEALLOCATE ms_crs_ind;

            RAISERROR(15472,-1,-1); 

            RETURN -1;

        END;

    WHILE @@fetch_status >= 0
        BEGIN

            IF EXISTS ( SELECT  1
                        FROM    @t
                        WHERE   ix_name = @ix_name )
                UPDATE  @t
                SET     column_name = column_name + ',' + @column_name
                WHERE   ix_name = @ix_name;

            ELSE
                INSERT  INTO @t
                        SELECT  @table_name ,
                                @schema_name ,
                                @fill_factor ,
                                @is_padded ,
                                @ix_name ,
                                @type ,
                                @keyno ,
                                @column_name ,
                                @cluster ,
                                @ignore_dupkey ,
                                @unique ,
                                @groupfile;

            FETCH ms_crs_ind INTO @table_name, @schema_name, @fill_factor,
                @is_padded, @ix_name, @type, @keyno, @column_name, @cluster,
                @ignore_dupkey, @unique, @groupfile;


        END;

    DEALLOCATE ms_crs_ind;


    SELECT  'CREATE ' + UPPER([unique]) + CASE WHEN [unique] = '' THEN ''
                                               ELSE ' '
                                          END + UPPER(cluster) + ' INDEX '
            + ix_name + ' ON ' + table_name + '(' + column_name + ')'
            + CASE WHEN fill_factor > 0
                        OR is_padded = 1
                        OR ( UPPER(cluster) != 'NONCLUSTERED'
                             AND ignore_dupkey = 'IGNORE_DUP_KEY'
                           )
                   THEN ' WITH ' + CASE WHEN is_padded = 1 THEN 'PAD_INDEX,'
                                        ELSE ''
                                   END
                        + CASE WHEN fill_factor > 0
                               THEN 'FILLFACTOR =' + LTRIM(fill_factor)
                               ELSE ''
                          END
                        + CASE WHEN ignore_dupkey = 'IGNORE_DUP_KEY'
                                    AND UPPER(cluster) = 'NONCLUSTERED'
                               THEN CASE WHEN ( fill_factor > 0
                                                OR is_padded = 1
                                              ) THEN ',IGNORE_DUP_KEY'
                                         ELSE ',IGNORE_DUP_KEY'
                                    END
                               ELSE ''
                          END
                   ELSE ''
              END + ' ON [' + groupfile + ']' AS col,Index_name=ix_name,index_keys=column_name
    FROM    @t;
    RETURN 0;
"""


def sync_index_name(tablename, old_index_name, new_index_name):
    """
    同步数据库索引sql
    """
    return f"""EXEC sp_rename '{tablename}.{old_index_name}', '{new_index_name}', 'INDEX';"""


def del_index_by_name(tablename, index_name):
    """
    删除数据库索引sql
    """
    return f"""DROP INDEX {tablename}.{index_name};"""


# source_server = sqlserver.SqlServerObject(host="172.17.18.110",port=1433,user="sa",password="CgSqlServerRoot2012",database="cgyypt") #开发服务器
# target_server = sqlserver.SqlServerObject(host="172.17.18.110",port=1433,user="sa",password="CgSqlServerRoot2012",database="cgyypt_2")  #部署服务器

# 主体sql框架
class MainData:
    """
    返回主要信息
    """

    # IDENTITY(1, 1)
    # PRIMARY
    # KEY
    # threading.Lock()
    def __init__(self, table_name: Union[str] = 'TAB', action: str = 'ADD', fields_info: Union[dict, list] = []):
        global source_server, target_server
        self.__main_sql = ''
        if action == 'CREATE':
            info = ""
            for i in fields_info:
                i['column_name'] = f"[{i['column_name']}]"
                if i['primary_key']:
                    info += (i['column_name'] + ' ' + i['data_type'] + ' NOT NULL PRIMARY KEY IDENTITY(1,1),') if i[
                        'identity_sql'] else (i['column_name'] + ' ' + i['data_type'] + ' NOT NULL PRIMARY KEY,')
                elif i['data_type'] == 'varchar' and i['max_lenght'] != '-1':
                    info += (i['column_name'] + ' ' + i['data_type'] + '(' + i['max_lenght'] + ')' + ' NULL,') if i[
                                                                                                                      'is_null'] == 'YES' else (
                            i['column_name'] + ' ' + i['data_type'] + '(' + i['max_lenght'] + ')' + ' NOT NULL,')
                elif (i['data_type'] == 'nvarchar' or i['data_type'] == 'varchar') and i['max_lenght'] == '-1':
                    info += (i['column_name'] + ' ' + i['data_type'] + '(MAX)' + ' NULL,') if i[
                                                                                                  'is_null'] == 'YES' else (
                            i['column_name'] + ' ' + i['data_type'] + '(MAX)' + ' NOT NULL,')
                elif i['data_type'] == 'nchar' and i['max_lenght'] != '-1':
                    info += (i['column_name'] + ' ' + i['data_type'] + '(' + i['max_lenght'] + ')' + ' NULL,') if i[
                                                                                                                      'is_null'] == 'YES' else (
                            i['column_name'] + ' ' + i['data_type'] + '(' + i['max_lenght'] + ')' + ' NOT NULL,')
                elif (i['data_type'] == 'nvarchar' or i['data_type'] == 'char') and i['max_lenght'] != '-1':
                    info += (i['column_name'] + ' ' + i['data_type'] + '(' + i['max_lenght'] + ')' + ' NULL,') if i[
                                                                                                                      'is_null'] == 'YES' else (
                            i['column_name'] + ' ' + i['data_type'] + '(' + i['max_lenght'] + ')' + ' NOT NULL,')
                elif i['data_type'] == 'date' or i['data_type'] == 'datetime' or i['data_type'] == 'datetime2' or i[
                    'data_type'] == 'time':
                    info += (i['column_name'] + ' ' + i['data_type'] + ' NULL,') if i['is_null'] == 'YES' else (
                            i['column_name'] + ' ' + i['data_type'] + ' NOT NULL,')
                elif i['data_type'] == 'int' or i['data_type'] == 'bigint' or i['data_type'] == 'bit' or i[
                    'data_type'] == 'tinyint' or i['data_type'] == 'smallint':
                    info += (i['column_name'] + ' ' + i['data_type'] + ' NULL,') if i['is_null'] == 'YES' else (
                            i['column_name'] + ' ' + i['data_type'] + ' NOT NULL,')
                elif i['data_type'] == 'decimal':
                    info += (i['column_name'] + ' ' + i['data_type'] + '(' + str(i['num_max']) + ',' + str(
                        i['num_min']) + ')' + ' NULL,') if i['is_null'] == 'YES' else (
                            i['column_name'] + ' ' + i['data_type'] + '(' + str(i['num_max']) + ',' + str(
                        i['num_min']) + ')' + ' NOT NULL,')
                elif i['data_type'] == 'money':
                    info += (i['column_name'] + ' ' + i['data_type'] + ' NULL,') if i['is_null'] == 'YES' else (
                            i['column_name'] + ' ' + i['data_type'] + ' NOT NULL,')
                elif i['data_type'] == "text":
                    info += (i['column_name'] + ' ' + i['data_type'] + ' NULL,') if i['is_null'] == 'YES' else (
                            i['column_name'] + ' ' + i['data_type'] + ' NOT NULL,')
                elif i['data_type'] == "varbinary" and i['max_lenght'] == "-1":
                    info += (i['column_name'] + ' ' + i['data_type'] + '(MAX)' + ' NULL,') if i[
                                                                                                  'is_null'] == 'YES' else (
                            i['column_name'] + ' ' + i['data_type'] + '(MAX)' + ' NOT NULL,')
                elif i['data_type'] == "varbinary" and i['max_lenght'] != "-1":
                    info += (i['column_name'] + ' ' + i['data_type'] + '(' + i['max_lenght'] + ')' + ' NULL,') if i[
                                                                                                                      'is_null'] == 'YES' else (
                            i['column_name'] + ' ' + i['data_type'] + '(' + i['max_lenght'] + ')' + ' NOT NULL,')
                else:
                    print(i)

            self.__main_sql = """CREATE TABLE %s(%s)""" % (table_name, info)
        else:
            fields_info['column_name'] = f"[{fields_info['column_name']}]"
            if fields_info['data_type'] == 'text':
                if action == 'ADD':

                    self.__main_sql = ("""ALTER TABLE [%s] ADD %s NULL""" % (table_name,
                                                                             fields_info['column_name'] + ' ' +
                                                                             fields_info['data_type']))
                elif action == 'ALTER':
                    self.__main_sql = """ALTER TABLE [%s] ALTER column %s NULL""" % (table_name, fields_info.get(
                        'column_name') + ' ' + fields_info['data_type'])
            elif fields_info['max_lenght'] and fields_info['num_max'] is None and fields_info['max_lenght'] != '-1':
                if action == 'ADD':
                    self.__main_sql = """ALTER TABLE [%s] ADD %s NULL""" % (table_name,
                                                                            fields_info['column_name'] + ' ' +
                                                                            fields_info['data_type'] + '(' + (
                                                                                fields_info['max_lenght']) + ')')
                elif action == 'ALTER':
                    self.__main_sql = """ALTER TABLE [%s] ALTER column %s NULL""" % (table_name, fields_info.get(
                        'column_name') + ' ' + fields_info['data_type'] + '(' + (fields_info['max_lenght']) + ')')
            elif fields_info['max_lenght'] is None and (
                    fields_info['data_type'] == 'int' or fields_info['data_type'] == 'tinyint' or fields_info[
                'data_type'] == 'bigint'):  # int类
                if action == 'ADD':
                    self.__main_sql = """ALTER TABLE [%s] ADD %s NULL""" % (
                        table_name, fields_info['column_name'] + ' ' + fields_info['data_type'])
                elif action == 'ALTER':
                    self.__main_sql = """ALTER TABLE [%s] ALTER column %s NULL""" % (
                        table_name, fields_info.get('column_name') + ' ' + fields_info['data_type'])
            elif fields_info['data_type'] == 'decimal':  # 金钱类
                if action == 'ADD':
                    self.__main_sql = """ALTER TABLE [%s] ADD %s NULL""" % (table_name,
                                                                            fields_info.get('column_name') + ' ' +
                                                                            fields_info['data_type'] + '(' + str(
                                                                                fields_info['num_max']) + ',' + str(
                                                                                fields_info['num_min']) + ')')
                elif action == 'ALTER':
                    self.__main_sql = """ALTER TABLE [%s] ALTER column %s NULL""" % (table_name, fields_info.get(
                        'column_name') + ' ' + fields_info['data_type'] + '(' + str(fields_info['num_max']) + ',' + str(
                        fields_info['num_min']) + ')')
            elif fields_info['data_type'] == 'money':
                if action == 'ADD':
                    self.__main_sql = """ALTER TABLE [%s] ADD %s NULL""" % (table_name,
                                                                            fields_info.get('column_name') + ' ' +
                                                                            fields_info['data_type'])
                elif action == 'ALTER':
                    self.__main_sql = """ALTER TABLE [%s] ALTER column %s NULL""" % (table_name, fields_info.get(
                        'column_name') + ' ' + fields_info['data_type'])
            elif fields_info['data_type'] == 'date' or fields_info['data_type'] == 'datetime' or fields_info[
                'data_type'] == 'datetime2' or fields_info[
                'data_type'] == 'time':
                if action == 'ADD':
                    self.__main_sql = """ALTER TABLE [%s] ADD %s NULL""" % (
                        table_name, fields_info.get('column_name') + ' ' + fields_info['data_type'])
                elif action == 'ALTER':
                    self.__main_sql = """ALTER TABLE [%s] ALTER column %s NULL""" % (
                        table_name, fields_info.get('column_name') + ' ' + fields_info['data_type'])
            elif fields_info['data_type'] == 'bit':
                if action == 'ADD':
                    self.__main_sql = """ALTER TABLE [%s] ADD %s NULL""" % (
                        table_name, fields_info.get('column_name') + ' ' + fields_info['data_type'])
                elif action == 'ALTER':
                    self.__main_sql = """ALTER TABLE [%s] ALTER column %s NULL""" % (
                        table_name, fields_info.get('column_name') + ' ' + fields_info['data_type'])
            elif fields_info['data_type'] == "varbinary" and fields_info['max_lenght'] == "-1":
                if action == 'ADD':
                    self.__main_sql = """ALTER TABLE [%s] ADD %s NULL""" % (
                        table_name, fields_info.get('column_name') + ' ' + fields_info['data_type'] + '(MAX)')
                elif action == 'ALTER':
                    self.__main_sql = """ALTER TABLE [%s] ALTER column %s NULL""" % (
                        table_name, fields_info.get('column_name') + ' ' + fields_info['data_type'] + '(MAX)')

            elif fields_info['data_type'] == "varbinary" and fields_info['max_lenght'] != "-1":
                if action == 'ADD':
                    self.__main_sql = """ALTER TABLE [%s] ADD %s NULL""" % (table_name,
                                                                            fields_info['column_name'] + ' ' +
                                                                            fields_info['data_type'] + '(' + (
                                                                                fields_info['max_lenght']) + ')')
                elif action == 'ALTER':
                    self.__main_sql = """ALTER TABLE [%s] ALTER column %s NULL""" % (table_name, fields_info.get(
                        'column_name') + ' ' + fields_info['data_type'] + '(' + (fields_info['max_lenght']) + ')')

    def __call__(self, *args, **kwargs):
        return self.__main_sql


# 拼接动态sql *args a=1,  **kwargs {a:1}
# class BaseBodyData:
#     """
#     socket返回消息体
#     """
#
#     def __init__(self, *args, **kwargs):
#         self.__body_data = {}
#
#     def __call__(self, *args, **kwargs):
#         return self.__body_data
# # 完成构造返回
# class DataBuild:
#     def __init__(self):
#         self.__result = {}
#         self.__main_data = {}
#         self.__data_body = {}
#
#     def create_main_data(self, main_data: MainData):
#         self.__main_data = main_data()
#         return self
#
#     def create_body_data(self, data_body: BaseBodyData):
#         if not self.__main_data:
#             raise Exception('未构造socket消息主体')
#         self.__data_body = data_body()
#         return self
#
#     def build_sql_result_data(self, **kwargs):
#         self.__main_data['data']['data'] = self.__data_body
#         self.__result.update(self.__main_data)
#         self.__result.update(kwargs)
#         return self.__result
# if __name__ == '__main__':
#     main_data = MainData("user", 'ADD')
#     # body_data = SocketAddFriendBodyData(tip=1, apply=1)
#     # body_data = SocketDataExceptBodyData('错了')
#     body_data = BaseBodyData({'txt': 123123})
#     build = DataBuild()
#     data = build.create_main_data(main_data).create_body_data(body_data).build_sql_result_data()
#     print('1111111111', data)


# if __name__ == '__main__':
#     source_servers = {'ip':'172.17.18.150\FENG','db':'yypt'}
#     target_servers = {'ip':'172.17.18.150\FENG','db':'yypt'}
#     sql = baobiao('yy_csh_ygxx', source_servers, target_server,{'id':1,"ygdm":'ccc', "ygmc":'踩踩踩'},'insert')
#     #sql = baobiao('yy_csh_ygxx', source_server, target_server,{'id':1,"ygdm":'ccc', "ygmc":'踩踩踩'},'update')
#     print(sql)


def handel_bytes_to_ten_six(bt: bytes):
    """
    二进制转十六进制
    '02X' 是格式规范，它指定了输出的格式。在这个格式规范中：
    0：表示使用零进行填充。
    2：表示输出的宽度为两位。
    X：表示将值转换为大写的十六进制表示形式。
    d：将值格式化为十进制整数。
    f：将值格式化为浮点数。
    s：将值格式化为字符串。
    x：将值格式化为小写的十六进制数。
    X：将值格式化为大写的十六进制数。
    b：将值格式化为二进制数。
    o：将值格式化为八进制数。
    e：将值格式化为科学计数法表示的浮点数。
    ord：将Ascii码转为Unicode码
    chr: 将Unicode码转为Ascii码
    """
    temp_b = ''.join(format(b, '02x') for b in bt)
    # for b in bt:
    #     temp_b += format(b, '02X')
    return '0x' + temp_b


def login_Info(func):
    def wrapper(request, *args, **kwargs):
        global source_server, target_server
        # print('连接信息')
        # print(source_server)
        # print(target_server)
        result = func(request, *args, **kwargs)
        return result

    return wrapper


# @login_Info
@require_http_methods(['POST'])
def selcet_ip(request):
    """
    连接ip 查询数据库列表
    """

    sql = """select * from sysdatabases where dbid>4"""
    global source_server, target_server
    json_data = json.loads(request.body)
    s_db_list = []
    t_db_list = []
    if json_data.get('source') and json_data.get('source').get('ip'):
        try:
            source_sql_server: sqlserver.SqlServerObject = sqlserver.SqlServerObject(host=json_data.get(
                'source')['ip'],
                                                                                     port=json_data.get(
                                                                                         'source')['port'],
                                                                                     user=json_data.get(
                                                                                         'source')['user'],
                                                                                     password=json_data.get(
                                                                                         'source')['password'],
                                                                                     database="master")  # 开发服务器
            s_db_name_set = source_sql_server.query_data(sql)[0]
            s_db_list = [obj[0] for obj in s_db_name_set]
            source_server = json_data.get('source')
        except:
            return JsonResponse({"code": 400, "message": "数据库连接失败", "data": {}})
    if json_data.get('target') and json_data.get('target').get('ip'):
        try:
            target_sql_server: sqlserver.SqlServerObject = sqlserver.SqlServerObject(host=json_data.get(
                'target')['ip'],
                                                                                     port=json_data.get(
                                                                                         'target')['port'],
                                                                                     user=json_data.get(
                                                                                         'target')['user'],
                                                                                     password=json_data.get(
                                                                                         'target')['password'],
                                                                                     database="master")  # 服务器
            t_db_name_set = target_sql_server.query_data(sql)[0]
            t_db_list = [obj[0] for obj in t_db_name_set]
            target_server = json_data.get('target')
        except:
            return JsonResponse({"code": 400, "message": "数据库连接失败", "data": {}})
    if s_db_list or t_db_list:
        return JsonResponse(
            {"code": 200, "message": "查询数据库列表成功", "data": {"source_db": s_db_list, "target_db": t_db_list}})
    else:
        return JsonResponse(
            {"code": 400, "message": "查询数据库列表失败", "data": {"source_db": s_db_list, "target_db": t_db_list}})


# @login_Info
@require_http_methods(['POST'])
def selcet_db(request):
    """
    选择数据库 返回表列表
    """
    # threading.Lock()
    print('选择db')
    global source_server, target_server
    source_sql_server = ''
    target_sql_server = ''
    json_data = json.loads(request.body)
    if json_data.get('source') and json_data.get('source').get('db'):
        try:
            source_sql_server: sqlserver.SqlServerObject = sqlserver.SqlServerObject(host=json_data.get(
                'source')['ip'],
                                                                                     port=json_data.get(
                                                                                         'source')['port'],
                                                                                     user=json_data.get(
                                                                                         'source')['user'],
                                                                                     password=json_data.get(
                                                                                         'source')['password'],
                                                                                     database=json_data.get(
                                                                                         'source').get('db'))  # 开发服务器
            source_server = json_data.get('source')
        except Exception as e:
            print(e)
            return JsonResponse({"code": 500, "message": "服务器连接失败", "data": str(e)})
    if json_data.get('target') and json_data.get('target').get('db'):
        try:
            target_sql_server: sqlserver.SqlServerObject = sqlserver.SqlServerObject(host=json_data.get(
                'target')['ip'],
                                                                                     port=json_data.get(
                                                                                         'target')['port'],
                                                                                     user=json_data.get(
                                                                                         'target')['user'],
                                                                                     password=json_data.get(
                                                                                         'target')['password'],
                                                                                     database=json_data.get(
                                                                                         'target').get('db'))  # 服务器
            target_server = json_data.get('target')
        except Exception as e:
            print(e)
            return JsonResponse({"code": 500, "message": "服务器连接失败", "data": str(e)})
    return JsonResponse({"code": 200, "message": "连接成功", "data": [str(source_sql_server), str(target_sql_server)]})


# 取所有数据库表
# @login_Info
def table_compare(request):
    global source_sql_server, target_sql_server

    if not source_server.get('db') or not target_server.get('db'):
        return JsonResponse({"code": 400, "message": "请先选择完两个数据库", "data": {}})
    try:
        source_sql_server = sqlserver.SqlServerObject(host=source_server['ip'],
                                                      port=source_server['port'],
                                                      user=source_server['user'],
                                                      password=source_server['password'],
                                                      database=source_server['db'])  # 开发服务器
        target_sql_server = sqlserver.SqlServerObject(host=target_server['ip'],
                                                      port=target_server['port'],
                                                      user=target_server['user'],
                                                      password=target_server['password'],
                                                      database=target_server['db'])  # 服务器
    except:
        response_data = {"code": 500, "message": "服务器内部错误"}
        return HttpResponse(json.dumps(response_data, ensure_ascii=False), content_type="application/json")
    sql_all_table_info = """SELECT a.TABLE_NAME as table_name,a.COLUMN_NAME,a.CHARACTER_MAXIMUM_LENGTH,a.DATA_TYPE ,a.NUMERIC_PRECISION,a.NUMERIC_SCALE  from INFORMATION_SCHEMA.columns a"""
    source_data, source_field = source_sql_server.query_data(sql_all_table_info)
    target_data, target_field = target_sql_server.query_data(sql_all_table_info)
    source_data_list: list = datahandle.sql_server_handle(source_field, source_data)
    target_data_list: list = datahandle.sql_server_handle(target_field, target_data)
    source_temp_list: set = set([obj.get('table_name') for obj in source_data_list])  # 开发服务器的所有表
    target_temp_list: set = set([obj.get('table_name') for obj in target_data_list])  # 部署服务器所有表
    # 拿到服务器上缺少的表名及自身有的 两个服务器的总和
    target_data_difference = source_data.difference(target_data)  # 开发数据库存在和服务器不存在的差异
    target_data_list_obj = datahandle.sql_server_handle(target_field, target_data_difference)  # 转字典 方便后面比对
    # 拿到服务器不存在的表 后面给前端判断此表是否是未创建
    difference_target_table = set(source_temp_list).difference(set(target_temp_list))
    # 拿到字段有变动的表名字
    table_change_list = set([obj.get('table_name') for obj in target_data_list_obj])
    # 初始化树列表 source数据库的所有表
    bd_list_obj = [({"table_name": i, "is_change": True if i in table_change_list else False, "not_create": False,
                     "selcet_sql": None}) for
                   i in source_temp_list]

    for obj in bd_list_obj:
        if obj.get("table_name") in difference_target_table:
            field_info_sql = """select COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE,NUMERIC_PRECISION, NUMERIC_SCALE from information_schema.COLUMNS where table_name = '%s'""" % obj.get(
                "table_name")
            identity_sql = """SELECT tb.name as '表名', t1.name as '字段名',case when  t4.id is null then 'false' else 'true' end as '是否主键', 
                                     case when  COLUMNPROPERTY( t1.id,t1.name,'IsIdentity') = 1 then 'true' else 'false' end as  '是否自增'
                                     ,t5.name as '类型' 
                                     ,cast(isnull(t6.value,'') as varchar(2000)) descr
                                    FROM SYSCOLUMNS t1
                                    left join SYSOBJECTS t2 on  t2.parent_obj = t1.id  AND t2.xtype = 'PK' 
                                    left join SYSINDEXES t3 on  t3.id = t1.id  and t2.name = t3.name  
                                    left join SYSINDEXKEYS t4 on t1.colid = t4.colid and t4.id = t1.id and t4.indid = t3.indid
                                    left join systypes  t5 on  t1.xtype=t5.xtype
                                    left join sys.extended_properties t6   on  t1.id=t6.major_id   and   t1.colid=t6.minor_id
                                    left join SYSOBJECTS tb  on  tb.id=t1.id
                                    where tb.name='%s' and t5.name<>'sysname' 
                                    order by t1.colid asc""" % obj.get("table_name")
            key_sql = """select * from information_schema.KEY_COLUMN_USAGE WHERE table_name='%s' """ % obj.get(
                "table_name")
            obj['not_create'] = True
            source_key_list = source_sql_server.query_table_field_sql(key_sql)
            source_identity_list = source_sql_server.query_data(identity_sql)[0]
            source_key_list = [obj[6] for obj in source_key_list]  # 主键字段
            source_identity_list = [obj[1] for obj in source_identity_list if obj[3] == "true"]  # 自增字段
            source_fieldname_list = source_sql_server.query_table_field_sql(field_info_sql)
            data_list: list = [{'column_name': str(obj[0]), 'data_type': obj[1],
                                'max_lenght': (str(obj[2]) if bool(obj[2]) is not False else obj[2]), 'is_null': obj[3],
                                'num_max': obj[4], 'num_min': obj[5],
                                'primary_key': (True if str(obj[0]) in source_key_list else False),
                                'identity_sql': (True if str(obj[0]) in source_identity_list else False)} for obj in
                               source_fieldname_list]
            obj['selcet_sql'] = MainData(obj.get("table_name"), 'CREATE', data_list)()

    response_data = {"code": 200, "message": "获取表成功", "data": bd_list_obj}
    # source_sql_server.close_db()
    # target_sql_server.close_db()
    return HttpResponse(json.dumps(response_data, ensure_ascii=False), content_type="application/json")


# 字段比较
# @login_Info
def table_field_compare(request):
    print('点击表取字段')
    json_result = request.GET
    json_result: dict
    table_name: str = json_result.get('table')
    try:
        source_sql_server: sqlserver.SqlServerObject = sqlserver.SqlServerObject(host=source_server['ip'],
                                                                                 port=source_server['port'],
                                                                                 user=source_server['user'],
                                                                                 password=source_server['password'],
                                                                                 database=source_server['db'])  # 开发服务器
        target_sql_server: sqlserver.SqlServerObject = sqlserver.SqlServerObject(host=target_server['ip'],
                                                                                 port=target_server['port'],
                                                                                 user=target_server['user'],
                                                                                 password=target_server['password'],
                                                                                 database=target_server['db'])  # 服务器
    except Exception:
        traceback.print_exc()
        response_data = {"code": 500, "message": "服务器内部错误"}
        return HttpResponse(json.dumps(response_data, ensure_ascii=False), content_type="application/json")
    # 获取新数据库的user表 所有字段类型信息
    field_info_sql = """select COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE,NUMERIC_PRECISION, NUMERIC_SCALE from information_schema.COLUMNS where table_name = '%s'""" % table_name
    key = """select * from information_schema.KEY_COLUMN_USAGE where table_name = '%s'""" % table_name
    identity_sql = """SELECT tb.name as '表名', t1.name as '字段名',case when  t4.id is null then 'false' else 'true' end as '是否主键', 
                         case when  COLUMNPROPERTY( t1.id,t1.name,'IsIdentity') = 1 then 'true' else 'false' end as  '是否自增'
                         ,t5.name as '类型' 
                         ,cast(isnull(t6.value,'') as varchar(2000)) descr
                        FROM SYSCOLUMNS t1
                        left join SYSOBJECTS t2 on  t2.parent_obj = t1.id  AND t2.xtype = 'PK' 
                        left join SYSINDEXES t3 on  t3.id = t1.id  and t2.name = t3.name  
                        left join SYSINDEXKEYS t4 on t1.colid = t4.colid and t4.id = t1.id and t4.indid = t3.indid
                        left join systypes  t5 on  t1.xtype=t5.xtype
                        left join sys.extended_properties t6   on  t1.id=t6.major_id   and   t1.colid=t6.minor_id
                        left join SYSOBJECTS tb  on  tb.id=t1.id
                        where tb.name='%s' and t5.name<>'sysname' 
                        order by t1.colid asc""" % table_name
    sp_helpindex_sql = """EXEC sp_helpindex [%s]""" % table_name  # 查询该表索引
    p_helpindex_sql = """EXEC p_helpindex [%s]""" % table_name  # 生成创建索引
    # 获取旧数据库的user表 所有字段类型信息
    try:
        source_fieldname_list = source_sql_server.query_table_field_sql(field_info_sql)
        source_fieldname_list_key = source_sql_server.query_table_field_sql(key)
        source_key = [obj[6] for obj in source_fieldname_list_key]
        source_identity_list = source_sql_server.query_data(identity_sql)[0]
        source_identity_list = [obj[1] for obj in source_identity_list if obj[3] == 'true']
    except:
        response_data = {"code": 500, "message": "开发服务器还没有这张表,无法比对字段，请先建表", "data": {}}
        return HttpResponse(json.dumps(response_data, ensure_ascii=False), content_type="application/json")
    try:
        target_fieldname_list = target_sql_server.query_table_field_sql(
            field_info_sql)  # [('id', 'int', None), ('name', 'text', 2147483647), ('address', 'text', 2147483647), ('phone', 'varchar', 255), ('hh', 'varchar', 255)]
        target_fieldname_list_key = target_sql_server.query_table_field_sql(key)
        target_key = [obj[6] for obj in target_fieldname_list_key]
        target_identity_list = target_sql_server.query_data(identity_sql)[0]
        target_identity_list = [obj[1] for obj in target_identity_list if obj[3] == 'true']
    except:
        response_data = {"code": 500, "message": "部署服务器还没有这张表,无法比对字段，请先建表", "data": {}}
        return HttpResponse(json.dumps(response_data, ensure_ascii=False), content_type="application/json")
    try:
        source_sql_server.insert_data(jiaoben)
    except Exception as e:
        if "There is already an object named 'p_helpindex" in str(e):
            print("有此过程脚本了")
    try:
        source_index_list = source_sql_server.query_table_field_sql(sp_helpindex_sql)
        s_helpindex_sql = source_sql_server.query_table_field_sql(p_helpindex_sql)  # 注意这里是找开发服务器的
    except Exception:
        return JsonResponse({"code": 500, "message": "开发服务器还没有这张表,无法比对字段，请先建表", "data": {}})
    try:
        target_sql_server.insert_data(jiaoben)
        # t_helpindex_sql = target_sql_server.query_table_field_sql(p_helpindex_sql)
    except Exception as e:
        if "There is already an object named 'p_helpindex" in str(e):
            print("有此过程脚本了")
        # t_helpindex_sql = target_sql_server.query_table_field_sql(p_helpindex_sql)
    try:
        target_index_list = target_sql_server.query_table_field_sql(sp_helpindex_sql)
        # t_helpindex_sql = target_sql_server.query_table_field_sql(p_helpindex_sql)
    except Exception:
        return JsonResponse({"code": 500, "message": "部署服务器还没有这张表,无法比对字段，请先建表", "data": {}})

    data_list: list = []
    for source_fieldname in source_fieldname_list:
        field_dict = dict()
        field_dict['table'] = table_name
        field_dict['field'] = str(source_fieldname[0])
        field_dict['source_type'] = {'column_name': str(source_fieldname[0]), 'data_type': source_fieldname[1],
                                     'max_lenght': (str(source_fieldname[2]) if bool(source_fieldname[2]) != False else
                                                    source_fieldname[2]), 'is_null': source_fieldname[3],
                                     'num_max': source_fieldname[4], 'num_min': source_fieldname[5],
                                     'primary_key': (True if str(source_fieldname[0]) in source_key else False),
                                     'identity_sql': (
                                         True if str(source_fieldname[0]) in source_identity_list else False)
                                     }
        field_dict['target_type'] = None  # 旧数据类型信息  此时初始化一下字典数据
        data_list.append(field_dict)
    data_list[0]['source_key'] = source_key
    data_list[0]['target_key'] = target_key
    # 获得新数据库的所有字段名 list返回 ['id', 'name', 'age', 'gender', 'xx']
    data_field: list = [data_dict.get('field') for data_dict in data_list]
    for target_fieldname in target_fieldname_list:
        # print(target_fieldname[0],'旧数据库返回元组中获取字段名')
        # 这里开始遍历旧数据库元组列表 并对比 与新数据字段区别
        if target_fieldname[0] in data_field:
            for i in data_list:
                if i.get('field') == target_fieldname[0]:
                    i['target_type'] = {'column_name': str(target_fieldname[0]), 'data_type': target_fieldname[1],
                                        'max_lenght': (
                                            str(target_fieldname[2]) if bool(target_fieldname[2]) != False else
                                            target_fieldname[2]), 'is_null': target_fieldname[3],
                                        'num_max': target_fieldname[4], 'num_min': target_fieldname[5],
                                        'primary_key': (True if str(target_fieldname[0]) in target_key else False),
                                        'identity_sql': (
                                            True if str(target_fieldname[0]) in target_identity_list else False)
                                        }
        else:
            field_dict = dict()
            field_dict['table'] = table_name
            field_dict['field'] = str(target_fieldname[0])
            field_dict['source_type'] = None  # 新数据类型信息  此时初始化一下字典数据
            field_dict['target_type'] = {'column_name': str(target_fieldname[0]), 'data_type': target_fieldname[1],
                                         'max_lenght': (
                                             str(target_fieldname[2]) if bool(target_fieldname[2]) != False else
                                             target_fieldname[2]), 'is_null': target_fieldname[3],
                                         'num_max': target_fieldname[4], 'num_min': target_fieldname[5],
                                         'primary_key': (True if str(target_fieldname[0]) in target_key else False),
                                         'identity_sql': (
                                             True if str(target_fieldname[0]) in target_identity_list else False)
                                         }
            data_list.append(field_dict)
    for obj in data_list:
        if obj.get('source_type') and not obj.get(
                'target_type'):  # 如果正式服务器没有该字段   就要 ALTER TABLE [{table_name}] ADD {data.get('column_name')} VARCHAR(45) NULL;
            temp_set = set(obj['source_type'].values())
            if temp_set:
                obj['sql_field'] = MainData(table_name, 'ADD', obj['source_type'])()  # 初始化sql 这里是新增字段属性
        elif obj.get('source_type') and obj.get(
                'target_type'):  # 如果正式服务器有，开发服务器也有  就要看差别修改字段 "ALTER TABLE [{table_name}] ALTER {data.get('column_name')} INT NULL ;
            temp_set = set(obj['source_type'].values()) - set(obj['target_type'].values())
            if temp_set:
                obj['sql_field'] = MainData(table_name, 'ALTER', obj['source_type'])()  # 初始化sql 这里是修改字段属性
        # 索引有差异的字段列表
    difference_set = set(source_index_list) - set(target_index_list)
    data_list[0]['sql_indexes'] = []
    target_index_name = {index_name: fileds.replace("(-)", " desc ") for index_name, info, fileds in target_index_list}
    bd_index_list = [i[2].replace("(-)", " desc ") for i in difference_set]
    target_has_index_dict = {filed: index_name for index_name, value, filed in target_index_list}
    # TODO 有时候索引名一样  但是字段却改变
    for k in s_helpindex_sql:
        k2 = k[2].replace(",", ", ")
        if k2 in bd_index_list:
            if k2 in target_has_index_dict and target_has_index_dict.get(k2) != k[1]:
                data_list[0]['sql_indexes'].append(sync_index_name(table_name, target_has_index_dict[k2], k[1]))
            else:
                if k[1] in target_index_name and k2 == target_index_name[k[1]]:
                    data_list[0]['sql_indexes'].append(del_index_by_name(table_name, k[1]) + k[0] + ";")
                elif k2 in target_index_name.values():
                    data_list[0]['sql_indexes'].append(
                        sync_index_name(table_name, target_has_index_dict[k2], k[1]) + ";")
                else:
                    data_list[0]['sql_indexes'].append(k[0] + ";")

    # source_sql_server.close_db()
    # target_sql_server.close_db()
    response_data = {"code": 200, "message": "成功", "data": data_list}
    return HttpResponse(json.dumps(response_data, ensure_ascii=False), content_type="application/json")


# 数据比较
# @login_Info
def table_data_compare(request):
    """
    获取数据差异
    """
    # threading.Lock()
    print('点击表取数据')
    json_result = request.GET
    table_name: str = json_result.get('table')
    try:
        source_sql_server: sqlserver.SqlServerObject = sqlserver.SqlServerObject(host=source_server['ip'],
                                                                                 port=source_server['port'],
                                                                                 user=source_server['user'],
                                                                                 password=source_server['password'],
                                                                                 database=source_server['db'])  # 开发服务器
        target_sql_server: sqlserver.SqlServerObject = sqlserver.SqlServerObject(host=target_server['ip'],
                                                                                 port=target_server['port'],
                                                                                 user=target_server['user'],
                                                                                 password=target_server['password'],
                                                                                 database=target_server['db'])  # 服务器
    except:
        response_data = {"code": 500, "message": "服务器内部错误"}
        return HttpResponse(json.dumps(response_data, ensure_ascii=False), content_type="application/json")
    table_sql = f"SELECT * FROM [{table_name}]"

    try:
        source_data, source_field = source_sql_server.query_data(table_sql)
        str_sql = ''
        for key in source_field:
            str_sql += key + ','
        table_sql = f"SELECT {str_sql[:len(str_sql) - 1]} FROM [{table_name}]"
    except Exception:
        response_data = {
            'code': 500,
            'message': '开发服务器还没有这张表,无法比对，请先建表',
            'data': {}
        }
        return HttpResponse(json.dumps(response_data, ensure_ascii=False), content_type="application/json")
    try:
        target_data, target_field = target_sql_server.query_data(table_sql)
    except pymssql.ProgrammingError as e:
        if "Invalid object name" in str(e):
            response_data = {"code": 500, "message": "部署服务器还没有这张表,无法比对，请先建表", "data": {}}
        else:
            response_data = {"code": 500, "message": "部署服务器字段未同步，请先同步字段再刷新查看数据差异", "data": {}}
        return HttpResponse(json.dumps(response_data, ensure_ascii=False), content_type="application/json")
    except:
        traceback.print_exc()
        response_data = {"code": 500, "message": "部署服务器还没有这张表,无法比对，请先建表", "data": {}}
        return HttpResponse(json.dumps(response_data, ensure_ascii=False), content_type="application/json")
    # difference方法 集合比对差异(差集) 返回差异值的集合
    source_data_difference = source_data.difference(target_data)  # 拿到开发数据库存在 服务器数据库不存在的
    target_data_difference = target_data.difference(source_data)  # 数据库（服务器）和新数据库的差异
    source_data_list: list = []
    target_data_list: list = []
    # temp: set = source_data - target_data
    if source_data_difference:
        # zip映射
        source_data_list: list = datahandle.sql_server_handle(source_field, source_data_difference)
    if target_data_difference:
        target_data_list: list = datahandle.sql_server_handle(target_field, target_data_difference)
    # 是否是自增字段
    identity_sql = """SELECT tb.name as '表名', t1.name as '字段名',case when  t4.id is null then 'false' else 'true' end as '是否主键', 
                             case when  COLUMNPROPERTY( t1.id,t1.name,'IsIdentity') = 1 then 'true' else 'false' end as  '是否自增'
                             ,t5.name as '类型' 
                             ,cast(isnull(t6.value,'') as varchar(2000)) descr
                            FROM SYSCOLUMNS t1
                            left join SYSOBJECTS t2 on  t2.parent_obj = t1.id  AND t2.xtype = 'PK' 
                            left join SYSINDEXES t3 on  t3.id = t1.id  and t2.name = t3.name  
                            left join SYSINDEXKEYS t4 on t1.colid = t4.colid and t4.id = t1.id and t4.indid = t3.indid
                            left join systypes  t5 on  t1.xtype=t5.xtype
                            left join sys.extended_properties t6   on  t1.id=t6.major_id   and   t1.colid=t6.minor_id
                            left join SYSOBJECTS tb  on  tb.id=t1.id
                            where tb.name='%s' and t5.name<>'sysname' 
                            order by t1.colid asc""" % table_name

    target_identity_list = [obj[1] for obj in target_sql_server.query_data(identity_sql)[0] if obj[3] == "true"]

    for obj in source_data_list:
        try:
            target_sql_id = f"SELECT id FROM [{table_name}] where id = %s" % obj['id']
            target_data = target_sql_server.query_data(target_sql_id)[0]  # 查找部署服务器上有重复的没 有就update  无就insert
        except:
            target_data = set()
        column = ''
        value = ''
        temp_sql_str = ''
        sql = None
        null = 'null'
        if len(target_data) == 0:  # insert
            for k, v in obj.items():
                column += f'{k},'
                if v is True:  # 这里存在 为None    所以必须判断 TRUE
                    value += f"""1,"""
                elif v is False:
                    value += f"""0,"""
                elif v == '':
                    value += "''" + ','
                elif isinstance(v, datetime.datetime):
                    value += "'" + v.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "'" + ','
                elif isinstance(v, datetime.date):
                    value += "'" + v.strftime("%Y-%m-%d") + "'" + ','
                elif isinstance(v, datetime.time):
                    value += "'" + v.strftime("%H:%I:%M") + "'" + ','
                elif isinstance(v, bytes):
                    value += handel_bytes_to_ten_six(v) + ','
                else:
                    value += f"""{(v if isinstance(v, int) else "'{}'".format(v)) if v or v == 0 else null},"""
            if value:
                if target_identity_list:
                    sql = f"SET IDENTITY_INSERT [{table_name}] ON  insert into [{table_name}]({column[:len(column) - 1]}) values ({value[:len(value) - 1]}) SET IDENTITY_INSERT [{table_name}] OFF"
                else:
                    sql = f"insert into [{table_name}]({column[:len(column) - 1]}) values ({value[:len(value) - 1]})"

        else:  # update
            for k, v in obj.items():
                k = f"[{k}]"
                if v is True and k != '[id]':  # 这里存在 为None    所以必须判断 TRUE
                    temp_sql_str += k + '=' + "1,"
                elif k == '[id]':
                    continue
                elif v is False:
                    temp_sql_str += k + '=' + "0,"
                elif v == '':
                    temp_sql_str += k + '=' + "'',"
                elif isinstance(v, datetime.datetime):
                    datetime_str = v.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    temp_sql_str += f"{k}='{datetime_str}',"
                elif isinstance(v, datetime.date):
                    temp_sql_str += k + '=' + "'" + v.strftime("%Y-%m-%d") + "'" + ','
                elif isinstance(v, datetime.time):
                    temp_sql_str += k + '=' + "'" + v.strftime("%H:%I:%M") + "'" + ','
                elif isinstance(v, bytes):
                    temp_sql_str += f"{k}={handel_bytes_to_ten_six(v)},"
                else:
                    temp_sql_str += k + '=' + (
                        "{}".format(v if isinstance(v, int) else f"'{v}'") if v or v == 0 else null) + ','
            if temp_sql_str:
                sql = f"update [{table_name}] set {temp_sql_str[:len(column) - 1]} where id = {obj['id']}"

        obj['sql'] = sql

    def handel_byte_fields(source: dict):
        let_dict = {}
        for _key, _value in source.items():
            if isinstance(_value, bytes):
                _value = f"BLOB({math.ceil(len(_value) / 1024 * 100) / 100}KB)二进制无法查看"
            let_dict[_key] = _value

        return let_dict

    source_data_list = list(map(lambda x: handel_byte_fields(x), source_data_list))
    target_data_list = list(map(lambda x: handel_byte_fields(x), target_data_list))

    response_data = {
        'code': 200,
        'message': '成功',
        'data': {
            'source_data': source_data_list,
            'target_data_list': target_data_list,
        }
    }
    if source_data_list is None:
        response_data = {
            'code': 400,
            'message': '该表为空数据',
            'data': {
                'source_data': [],
                'target_data_list': target_data_list,
            }
        }
    # source_sql_server.close_db()
    # target_sql_server.close_db()
    return JsonResponse(response_data)


# 通用同步
@require_http_methods(['POST'])
# @login_Info
def auto_sync(request):
    global target_server
    target_sql_server = sqlserver.SqlServerObject(host=target_server['ip'],
                                                  port=target_server['port'],
                                                  user=target_server['user'],
                                                  charset='UTF-8',
                                                  password=target_server['password'],
                                                  database=target_server['db'])  # 服务器
    json_data = json.loads(request.body)
    sql: str = json_data.get('sql')
    err_list = []
    success_list = []
    try:
        target_sql_server.insert_data(sql)
        success_list.append(sql)
    except Exception as e:
        traceback.print_exc()
        err = str(e)
        if '(1505,' in err:
            value = err[err.find("The duplicate key value is "):err.find(".DB")]
            err_list.append('该唯一索引无法创建，因为该字段有重复数据; 该表需要相关人员参与' + value)
        err_list.append(sql + ',')
    response_data = {
        'code': 200,
        'message': '请查看结果',
        'data': {"success": success_list, "error": err_list}
    }
    return JsonResponse(response_data)


# 获取报表数据
@require_http_methods(['GET'])
@xframe_options_exempt
# @login_Info
def get_statement_data(request):
    """
    下载依赖包 pip install requests -i https://pypi.tuna.tsinghua.edu.cn/simple
    :param request:
    :return:
    """
    headers = {
        'User - Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36',
    }
    url = 'http://172.17.18.150:53300/api/reports/1?format=html&inline=true'
    response = requests.get(url=url, headers=headers)
    response.encoding = 'utf-8'
    pprint(response.text)
    return HttpResponse(response)


# 数据同步 弃用
@require_http_methods(['POST'])
def data_synchronization(request):
    json_result = json.loads(request.body)
    table_name: str = json_result.get('table')
    json_result: dict
    try:
        target_sql_server = sqlserver.SqlServerObject('172.17.18.114', 1433, 'sa', 'sa1234', 'old', 'utf8',
                                                      as_dict=False, autocommit=True)
    except Exception as e:
        response_data = {"code": 500, "message": "服务器内部错误"}
        return HttpResponse(json.dumps(response_data, ensure_ascii=False), content_type="application/json")
    target_sql = f"SELECT * FROM [{table_name}] where id = '%s'" % json_result.get('data').get('id')
    try:
        target_data, target_field = target_sql_server.query_data(target_sql)
    except pymssql.ProgrammingError:
        response_data = {
            'code': 400,
            'message': '该表不存在',
            'data': {"source_data": [], "target_data_list": []}
        }
        return HttpResponse(json.dumps(response_data, ensure_ascii=False), content_type="application/json")
    target_data: set
    temp_sql_str = str()
    column = ''
    value = list()
    for k, v in json_result.get('data').items():
        value.append(v)
        column += k + ','
        if v:
            data_sql: str = k + "=%s," % (v if v and type(v) is int else "'" + v + "'")
            temp_sql_str += data_sql
    temp_sql_str: str = temp_sql_str[0:len(temp_sql_str) - 1]
    column: str = column[0:len(column) - 1]
    if not target_data:
        sql = f"insert into [{table_name}]({column}) values {str(tuple(value))}"
    else:
        sql = f"update [{table_name}] set {temp_sql_str} where id = {json_result.get('data').get('id')}"

    try:
        target_sql_server.insert_data(sql)
    except Exception as e:
        response_data = {"code": 500, "message": "此条数据异常,字段可能不同步"}
        return HttpResponse(json.dumps(response_data, ensure_ascii=False), content_type="application/json")
    response_data = {'code': 200, 'message': '成功'}
    return HttpResponse(json.dumps(response_data, ensure_ascii=False), content_type="application/json")


@require_http_methods(['POST'])
def get_change_table(request):
    # 拿到数据有变动的表  浪费性能弃用 后续改为多进程版本
    global source_sql_server, target_sql_server
    is_change = True
    return JsonResponse({'code': 200, 'message': '成功', 'data': is_change})
