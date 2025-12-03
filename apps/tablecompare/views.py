import datetime
import json
import math
import re
import traceback
from concurrent.futures import ProcessPoolExecutor
from functools import wraps
from multiprocessing import Manager
from typing import Union

import pymssql
from django.db import transaction
from django.db.models import Max
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from rest_framework.request import Request
from rest_framework.viewsets import ViewSet

from .handel_insert_update import process_data
from .models import SysSyncTable, SysSyncTableDetail, SyncLogTable
from .ssh import jiaoben
from .util import sqlserver, datahandle

login_server = {}
online_list = []
source_server = {}
target_server = {}


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


def edit_primary_key_constraint(tablename: str, index_name: str, CLUSTERED: bool, field: str):
    """
    修改约束
    """
    return f"""ALTER TABLE {tablename} ADD CONSTRAINT {index_name} PRIMARY KEY {'CLUSTERED' if CLUSTERED else 'NONCLUSTERED '}
                ({field}) WITH( STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];"""


def cancel_primary_key_constraint(tablename, index_name):
    """
    取消约束
    """
    return f"""ALTER TABLE [{tablename}] DROP CONSTRAINT {index_name};"""


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


def handel_update_dict_to_sql(_dict: dict) -> str:
    set_clause = ""
    for k, v in _dict.items():
        k = f"[{k}]"
        if v is True and k != '[id]':  # 这里存在 为None    所以必须判断 TRUE
            set_clause += k + '=' + "1,"
        elif k == '[id]':
            continue
        elif v is False:
            set_clause += k + '=' + "0,"
        elif v == '':
            set_clause += k + '=' + "'',"
        elif isinstance(v, datetime.datetime):
            datetime_str = v.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            set_clause += f"{k}='{datetime_str}',"
        elif isinstance(v, datetime.date):
            set_clause += k + '=' + "'" + v.strftime("%Y-%m-%d") + "'" + ','
        elif isinstance(v, datetime.time):
            set_clause += k + '=' + "'" + v.strftime("%H:%I:%M") + "'" + ','
        elif isinstance(v, bytes):
            set_clause += f"{k}={handel_bytes_to_ten_six(v)},"
        else:
            set_clause += k + '=' + (
                "{}".format(v if isinstance(v, int) else f"'{v.encode('latin1').decode('gbk')}'") if v or v == 0 else 'null') + ','
    return set_clause


# 主体sql框架
class MainData:
    """
    返回主要信息
    """

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
                    'data_type'] == 'tinyint' or i['data_type'] == 'smallint' or i['data_type'] == 'float':
                    info += (i['column_name'] + ' ' + i['data_type'] + ' NULL,') if i['is_null'] == 'YES' else (
                        (i['column_name'] + ' ' + i['data_type'] + ' NOT NULL IDENTITY(1,1),') if i[
                            'identity_sql'] else (i['column_name'] + ' ' + i['data_type'] + ' NOT NULL,'))
                elif i['data_type'] == 'decimal' or i['data_type'] == 'numeric':
                    info += (i['column_name'] + ' ' + i['data_type'] + '(' + str(i['num_max']) + ',' + str(
                        i['num_min']) + ')' + ' NULL,') if i['is_null'] == 'YES' else (
                            i['column_name'] + ' ' + i['data_type'] + '(' + str(i['num_max']) + ',' + str(
                        i['num_min']) + ')' + ' NOT NULL,')
                elif i['data_type'] == 'money':
                    info += (i['column_name'] + ' ' + i['data_type'] + ' NULL,') if i['is_null'] == 'YES' else (
                            i['column_name'] + ' ' + i['data_type'] + ' NOT NULL,')
                elif i['data_type'] == "text" or i['data_type'] == 'ntext':
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
                    print(table_name, i, '没有处理该类型字段')

            self.__main_sql = """CREATE TABLE %s(%s)""" % (table_name, info)
        else:
            fields_info['column_name'] = f"[{fields_info['column_name']}]"
            if fields_info['data_type'] == 'text':
                if action == 'ADD':

                    self.__main_sql = ("""ALTER TABLE [%s] ADD %s NULL""" % (table_name,
                                                                             fields_info['column_name'] + ' ' +
                                                                             fields_info['data_type']))
                elif action == 'ALTER':
                    self.__main_sql = f"""ALTER TABLE [%s] ALTER column %s NULL """ % (table_name, fields_info.get(
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
            elif fields_info['data_type'] == 'decimal' or fields_info['data_type'] == 'numeric':  # 金钱类
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
            if action == 'ALTER':
                self.__main_sql += "  " + self.__main_sql.replace('NULL', 'NULL' if fields_info[
                                                                                        'is_null'] == 'YES' else 'NOT NULL')

    def __call__(self, *args, **kwargs):
        return self.__main_sql


class SYNCSQL:
    def __init__(self, target_connetion):
        self.target_connetion: sqlserver.SqlServerObject = target_connetion


class ALTER(SYNCSQL):
    def __call__(self, sql: str):
        ADD = sql.find('ADD') != -1
        if ADD:
            # 正则表达式模式
            pattern = r"ALTER TABLE \[(?P<table_name>[^\]]+)\] ADD \[(?P<column_name>[^\]]+)\]"

            # 使用正则表达式进行匹配
            match = re.match(pattern, sql)

            # 提取匹配的表名和列名
            if match:
                table_name = match.group("table_name")
                column_name = match.group("column_name")
                print(f"表名: {table_name}")
                print(f"列名: {column_name}")
            else:
                table_name = "未匹配到"
                column_name = "未匹配到"
            # return '表结构新增字段', """ALTER TABLE [{table}] DROP COLUMN [{column}];""".format(table=table_name, column=column_name), table_name
            return '表结构新增字段', """手动删除 [{table}] 列 [{column}];""".format(table=table_name,
                                                                        column=column_name), table_name
        else:
            pattern = r"ALTER TABLE \[(?P<table_name>[^\]]+)\] ALTER column \[(?P<column_name>[^\]]+)\]"

            # 使用正则表达式进行匹配
            match = re.findall(pattern, sql)
            result_join = ''
            result = None
            for cmd in match:
                # 提取匹配的表名和列名
                if match:
                    table_name = cmd[0]
                    column_name = cmd[1]
                else:
                    table_name = "未匹配到"
                    column_name = "未匹配到"
                query_sql = f"""SELECT COLUMN_NAME, DATA_TYPE, character_maximum_length
                            FROM INFORMATION_SCHEMA.COLUMNS
                            WHERE TABLE_NAME = '{table_name}' AND COLUMN_NAME = '{column_name}'
                        """
                try:
                    result = self.target_connetion.sqlserver_data(query_sql)[0]
                    result = {k: v.encode('latin1').decode('gbk') if isinstance(v, str) else v for k, v in result.items()}
                except KeyError:
                    result = {'DATA_TYPE': '未知'}
                if result['DATA_TYPE'] == 'varchar':
                    result_join += f"""ALTER TABLE [{table_name}] ALTER column [{column_name}] {result['DATA_TYPE']}({result['character_maximum_length']}) NULL """
                else:
                    result_join += f"""ALTER TABLE [{table_name}] ALTER column [{column_name}] {result['DATA_TYPE']} NULL """

            return result, result_join, table_name


class INSERT(SYNCSQL):
    def __call__(self, sql):
        pattern = r"SET\s+IDENTITY_INSERT\s+\[([^\]]+)\]\s+ON\s+insert\s+into\s+\[([^\]]+)\]\s*\((.*?)\)\s+values\s+\((.*?)\)\s+SET\s+IDENTITY_INSERT\s+\[([^\]]+)\]\s+OFF"
        match = re.match(pattern, sql)
        if match:
            table_name = match.group(1)  # 提取表名
            insert_into_table = match.group(2)  # 提取 INSERT INTO 子句中的表名
            values_clause = match.group(3)  # 提取 VALUES 子句中的值
            set_identity_insert_off = match.group(4)  # 提取 SET IDENTITY_INSERT OFF 子句中的表名
        else:
            return "未找到匹配", '', None
        return '新增', '需要人工删除', table_name


class UPDATE(SYNCSQL):
    def __call__(self, sql):
        """
        :param sql:
        :return:
        """
        pattern = r"update\s+\[([^\]]+)\]\s+set\s+(.*?)\s+where\s+(.*?)$"
        match = re.match(pattern, sql)
        if match:
            table_name = match.group(1)  # 提取表名
            set_clause = match.group(2)  # 提取 SET 子句
            where_clause = match.group(3)  # 提取 WHERE 子句
        else:
            return '无法还原', '未匹配到', sql
        query_sql = f"""SELECT * FROM [{table_name}] WHERE {where_clause} """
        try:
            result = self.target_connetion.sqlserver_data(query_sql)[0]
            set_clause = handel_update_dict_to_sql(result)
        except Exception as e:
            print(e)
            return '无数据', '未匹配到', table_name
        # result = {k: v.encode('latin1').decode('gbk') if isinstance(v, str) else v for k, v in result.items()}
        return result, "update [{}] set {} where id = {}".format(table_name, set_clause[:-1], result['id']), table_name


def login_wraps(func):
    """
    登录装饰器 支持多人使用
    """

    @wraps(func)
    def inner(request, *args, **kwargs):
        global source_server, target_server, online_list
        try:
            json_data = json.loads(request.body)
            print(json_data)
        except Exception as e:
            print('login_wraps', e)
            json_data = {}
        if not login_server.get(request.META['REMOTE_ADDR']):
            login_server[request.META['REMOTE_ADDR']] = {'source_server': None, 'target_server': None}
        if json_data.get('source'):
            login_server[request.META['REMOTE_ADDR']]['source_server'] = json_data.get('source')
        if json_data.get('target'):
            login_server[request.META['REMOTE_ADDR']]['target_server'] = json_data.get('target')
        online_list.append(request.META['REMOTE_ADDR'])
        online_list = list(set(online_list))
        source_server = login_server.get(request.META['REMOTE_ADDR']).get('source_server', {})
        target_server = login_server.get(request.META['REMOTE_ADDR']).get('target_server', {})
        return func(request, *args, **kwargs)

    return inner


def lock_wraps(func):
    """
    登录装饰器 支持多人使用
    """

    @wraps(func)
    def inner(request, *args, **kwargs):
        if request.META['REMOTE_ADDR'] in online_list or not online_list:
            return func(request, *args, **kwargs)
        return JsonResponse({'code': 400, 'message': '他人正在使用'})

    return inner


def log_generate(plan_id=None, sql_list=[], target_connetion=None, REMOTE_ADDR=None):
    sql_dict = {'ALTER': ALTER(target_connetion), 'update': UPDATE(target_connetion),
                'insert': INSERT(target_connetion)}
    batch_query = SyncLogTable.objects.select_for_update().last()
    if batch_query:
        batch_id = batch_query.id
    else:
        batch_id = 0
    for sql in sql_list:
        print(sql, ',')
        if sql.startswith('ALTER'):
            call_func = sql_dict.get('ALTER')
        elif sql.startswith('update'):
            call_func = sql_dict.get('update')
        elif sql.startswith('insert') or sql.startswith('SET IDENTITY_INSERT'):
            call_func = sql_dict.get('insert')
        else:
            call_func = type('CUSTOMIZE', (), {'__call__': lambda self, sql: ('没有该sql匹配项', sql, sql)})()
        old_data, old_sql, table_name = call_func(sql)
        SyncLogTable.objects.create(jlrq=datetime.datetime.now(),
                                    table_name=table_name,
                                    source_data=old_data,
                                    recover_sql=old_sql,
                                    exec_sql=sql,
                                    plan_id=plan_id,
                                    recover_batch=batch_id,
                                    czy_ip=REMOTE_ADDR)


@require_http_methods(['POST'])
@lock_wraps
@login_wraps
def selcet_ip(request):
    """
    连接ip 查询数据库列表
    """

    sql = """select name from sysdatabases where dbid>4"""
    global source_server, target_server
    json_data = json.loads(request.body)
    s_db_list = []
    t_db_list = []
    if json_data.get('source') and json_data.get('source').get('ip'):
        try:
            json_data['source'].pop('zd', '')
            source_server = json_data.get('source')
            source_sql_server = sqlserver.SqlServerObject(dbname='db1',
                                                          host=json_data.get('source')['ip'],
                                                          ssh_host=json_data.get(
                                                              'source').get('ssh_ip'),
                                                          database="master",
                                                          **json_data.get(
                                                              'source'))  # 开发服务器
            source_sql_server.set_dynamic_db()
            s_db_name_set = source_sql_server.query_data(sql)[0]
            s_db_list = [obj[0] for obj in s_db_name_set]
        except:
            return JsonResponse({"code": 400, "message": "数据库连接失败", "data": {}})
    if json_data.get('target') and json_data.get('target').get('ip'):
        try:
            json_data['target'].pop('zd', '')
            target_server = json_data.get('target')
            target_sql_server = sqlserver.SqlServerObject(
                dbname='db2',
                host=json_data.get('target').get('ip'),
                ssh_host=json_data.get('target').get('ssh_ip'),
                database="master",
                **json_data.get(
                    'target')
            )  # 服务器
            target_sql_server.set_dynamic_db()
            t_db_name_set = target_sql_server.query_data(sql)[0]
            t_db_list = [obj[0] for obj in t_db_name_set]
        except:
            return JsonResponse({"code": 400, "message": "数据库连接失败", "data": {}})
    if s_db_list or t_db_list:
        return JsonResponse(
            {"code": 200, "message": "查询数据库列表成功", "data": {"source_db": s_db_list, "target_db": t_db_list}})
    else:
        return JsonResponse(
            {"code": 400, "message": "查询数据库列表失败", "data": {"source_db": s_db_list, "target_db": t_db_list}})


@require_http_methods(['POST'])
@lock_wraps
@login_wraps
def selcet_db(request):
    """
    选择数据库 返回表列表
    """
    # threading.Lock()
    print('选择db')
    global source_server, target_server
    json_data = json.loads(request.body)
    if json_data.get('source') and json_data.get('source').get('db'):
        try:
            json_data['source'].pop('zd', '')
            source_server = json_data.get('source')
            source_sql_server = sqlserver.SqlServerObject(dbname='db1',
                                                          host=json_data.get('source')['ip'],
                                                          ssh_host=json_data.get('source').get('ssh_ip'),
                                                          database=json_data.get('source').get('db'),
                                                          **json_data.get('source'))  # 开发服务器
            source_sql_server.set_dynamic_db()
        except Exception as e:
            return JsonResponse({"code": 500, "message": "服务器连接失败", "data": str(e)})
    if json_data.get('target') and json_data.get('target').get('db'):
        try:
            json_data['target'].pop('zd', '')
            target_server = json_data.get('target')
            target_sql_server = sqlserver.SqlServerObject(dbname='db2',
                                                          host=json_data.get('target').get('ip'),
                                                          ssh_host=json_data.get('target').get('ssh_ip'),
                                                          database=json_data.get('target').get('db'),
                                                          **json_data.get('target'))  # 服务器
            target_sql_server.set_dynamic_db()

        except Exception as e:
            return JsonResponse({"code": 500, "message": "服务器连接失败", "data": str(e)})
    return JsonResponse({"code": 200, "message": "连接成功", "data": []})


# 取所有数据库表
@lock_wraps
@login_wraps
def table_compare(request):
    global source_server, target_server
    table_name = request.GET.get('search', '')
    if not source_server.get('db') or not target_server.get('db'):
        return JsonResponse({"code": 400, "message": "请先选择完两个数据库", "data": {}})
    try:
        source_sql_server = sqlserver.SqlServerObject(dbname='db1',
                                                      host=source_server['ip'],
                                                      database=source_server['db'],
                                                      ssh_host=source_server.get('ssh_ip'),
                                                      **source_server)  # 开发服务器
        target_sql_server = sqlserver.SqlServerObject(dbname='db2',
                                                      host=target_server['ip'],
                                                      database=target_server['db'],
                                                      ssh_host=target_server.get('ssh_ip'),
                                                      **target_server)  # 服务器
    except Exception as e:
        traceback.print_exc()
        return JsonResponse({"code": 500, "message": "服务器内部错误"})
    sql_all_table_info = """SELECT a.TABLE_NAME as table_name,a.COLUMN_NAME,a.CHARACTER_MAXIMUM_LENGTH,a.DATA_TYPE ,a.NUMERIC_PRECISION,a.NUMERIC_SCALE  from INFORMATION_SCHEMA.columns a"""
    if table_name:
        sql_all_table_info += f""" WHERE a.TABLE_NAME like '{table_name}%'"""

    sql_all_table_info += """ ORDER by a.TABLE_NAME"""

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
                     "selcet_sql": None}) for i in source_temp_list if i.lower() != 'sys_from_customizequery']

    for obj in bd_list_obj:
        obj['table_name'] = obj.get('table_name').lower()
        if obj['table_name'] == 'sys_from_customizequery':
            continue
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

    bd_list_obj = sorted(bd_list_obj, key=lambda x: x['table_name'])

    return JsonResponse({"code": 200, "message": "获取表成功", "data": bd_list_obj})


# 字段比较
@login_wraps
def table_field_compare(request):
    print('点击表取字段')
    json_result = request.GET
    table_name: str = json_result.get('table')
    global source_server, target_server
    try:
        source_sql_server = sqlserver.SqlServerObject(dbname='db1',
                                                      host=source_server['ip'],
                                                      database=source_server['db'],
                                                      ssh_host=source_server.get('ssh_ip'),
                                                      **source_server)  # 开发服务器
        target_sql_server = sqlserver.SqlServerObject(dbname='db2',
                                                      host=target_server['ip'],
                                                      database=target_server['db'],
                                                      ssh_host=target_server.get('ssh_ip'),
                                                      **target_server)  # 服务器
    except Exception:
        traceback.print_exc()
        response_data = {"code": 500, "message": "服务器内部错误"}
        return JsonResponse(response_data)
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
        return JsonResponse(response_data)
    try:
        target_fieldname_list = target_sql_server.query_table_field_sql(
            field_info_sql)  # [('id', 'int', None), ('name', 'text', 2147483647), ('address', 'text', 2147483647), ('phone', 'varchar', 255), ('hh', 'varchar', 255)]
        target_fieldname_list_key = target_sql_server.query_table_field_sql(key)
        target_key = [obj[6] for obj in target_fieldname_list_key]
        target_identity_list = target_sql_server.query_data(identity_sql)[0]
        target_identity_list = [obj[1] for obj in target_identity_list if obj[3] == 'true']
    except:
        response_data = {"code": 500, "message": "部署服务器还没有这张表,无法比对字段，请先建表", "data": {}}
        return JsonResponse(response_data)
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
    # target_index_name = {index_name: field.replace("(-)", " desc ") for index_name, info, field in target_index_list}
    bd_index_dict = {index_name: field.replace("(-)", " desc ") for index_name, info, field in difference_set}
    target_has_index_dict = {index_name: filed.replace("(-)", " desc ") for index_name, value, filed in
                             target_index_list}
    target_has_filed_dict = {filed.replace("(-)", " desc "): index_name for index_name, value, filed in
                             target_index_list}
    primary_key = """SELECT 
                            i.name AS IndexName,
                            i.is_primary_key AS IsPrimaryKey
                            FROM 
                                sys.indexes AS i
                            INNER JOIN 
                                sys.objects AS o ON i.object_id = o.object_id
                            WHERE 
                            o.type = 'U' 
                            AND o.name = '{table_name}' 
                            AND i.is_primary_key = 1
                                    """

    SPRIMARY_LIST = [tup[0] for tup in
                     source_sql_server.query_table_field_sql(primary_key.format(table_name=table_name))]
    TPRIMARY_LIST = [tup[0] for tup in
                     target_sql_server.query_table_field_sql(primary_key.format(table_name=table_name))]
    for k in s_helpindex_sql:
        create_index_sql = k[0].replace("ON [PRIMARY]",
                                        "") + " WITH (STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY];"
        k2 = k[2].replace(",", ", ")  # 字段+排序名字， 有联合索引
        index_name = k[1]  # 索引名
        CLUSTERED = k[0].find('UNIQUE CLUSTERED') > 0  # 是否聚集
        # indid 为 1 则是主键约束
        SPRIMARY = index_name in SPRIMARY_LIST
        TPRIMARY = index_name in TPRIMARY_LIST
        if index_name in bd_index_dict:
            if index_name in target_has_index_dict and not TPRIMARY and SPRIMARY:
                # 索引名字一样 并且不是主键索引 就不管字段了， 直接删除再创建约束
                data_list[0]['sql_indexes'].append(
                    del_index_by_name(table_name, index_name) + edit_primary_key_constraint(table_name,
                                                                                            index_name,
                                                                                            CLUSTERED, k2))
            elif index_name in target_has_index_dict and TPRIMARY and SPRIMARY:
                # 索引名字一样 并且是主键索引 无法删除 先取消主键约束再创建主键约束
                data_list[0]['sql_indexes'].append(
                    cancel_primary_key_constraint(table_name, index_name) + edit_primary_key_constraint(table_name,
                                                                                                        index_name,
                                                                                                        CLUSTERED, k2))
            elif index_name not in target_has_index_dict and TPRIMARY and SPRIMARY:
                # 索引名字不在目标服务器 并且是主键索引 先看看目标服务器的主键是不是叫其他名字
                if k2 in target_has_filed_dict:
                    # 如果 索引名字不一样但是字段一致则直接修改索引名字就好
                    data_list[0]['sql_indexes'].append(
                        sync_index_name(table_name, target_has_filed_dict[k2], index_name))
                else:
                    # 如果连字段都不存在 则直接添加主键约束
                    data_list[0]['sql_indexes'].append(edit_primary_key_constraint(table_name,
                                                                                   index_name,
                                                                                   CLUSTERED, k2))
            elif index_name not in target_has_index_dict and not TPRIMARY and SPRIMARY:
                # 索引名字不在目标服务器 但不是主键索引
                if k2 in target_has_filed_dict:
                    # 如果字段一致 则删除 重新创建
                    if target_has_filed_dict[k2] in TPRIMARY_LIST:
                        data_list[0]['sql_indexes'].append(
                            cancel_primary_key_constraint(table_name,
                                                          target_has_filed_dict[k2]) + edit_primary_key_constraint(
                                table_name,
                                index_name,
                                CLUSTERED, k2))
                    else:
                        data_list[0]['sql_indexes'].append(
                            del_index_by_name(table_name, target_has_filed_dict[k2]) + edit_primary_key_constraint(
                                table_name,
                                index_name,
                                CLUSTERED, k2))
                else:
                    # 如果连字段都不存在 则直接创建
                    data_list[0]['sql_indexes'].append(edit_primary_key_constraint(
                        table_name,
                        index_name,
                        CLUSTERED, k2))
            elif index_name not in target_has_index_dict and not TPRIMARY and not SPRIMARY:
                # 索引名字不在目标服务器 但不是主键索引
                if k2 in target_has_filed_dict:
                    # 如果字段一致 则删除 重新创建
                    if target_has_filed_dict[k2] in TPRIMARY_LIST:
                        data_list[0]['sql_indexes'].append(
                            cancel_primary_key_constraint(table_name,
                                                          target_has_filed_dict[k2]) + create_index_sql)
                    else:
                        data_list[0]['sql_indexes'].append(
                            del_index_by_name(table_name, target_has_filed_dict[k2]) + create_index_sql)
                else:
                    # 如果连字段都不存在 则直接创建
                    data_list[0]['sql_indexes'].append(create_index_sql)
            elif index_name in target_has_index_dict and not TPRIMARY and not SPRIMARY:
                # 索引名字在目标服务器 但不是主键索引
                data_list[0]['sql_indexes'].append(del_index_by_name(table_name, index_name) + create_index_sql)
            else:
                print('没有处理到的索引')
                data_list[0]['sql_indexes'].append(create_index_sql)
    response_data = {"code": 200, "message": "成功", "data": data_list}
    print(data_list)
    return JsonResponse(response_data)


# 数据比较
@login_wraps
def table_data_compare(request):
    """
    获取数据差异
    """
    print('点击表取数据')
    json_result = request.GET
    table_name: str = json_result.get('table')
    try:
        source_sql_server = sqlserver.SqlServerObject(dbname='db1',
                                                      host=source_server['ip'],
                                                      database=source_server['db'],
                                                      ssh_host=source_server.get('ssh_ip'),
                                                      **source_server)  # 开发服务器
        target_sql_server = sqlserver.SqlServerObject(dbname='db2',
                                                      host=target_server['ip'],
                                                      database=target_server['db'],
                                                      ssh_host=target_server.get('ssh_ip'),
                                                      **target_server)  # 服务器
    except Exception as e:
        return JsonResponse({"code": 500, "message": "服务器内部错误"})
    table_sql = f"SELECT * FROM [{table_name}]"

    try:
        source_data, source_field = source_sql_server.query_data(table_sql)
        str_sql = ''
        for key in source_field:
            str_sql += key + ','
        table_sql = f"SELECT {str_sql[:len(str_sql) - 1]} FROM [{table_name}]"
    except Exception:
        return JsonResponse({
            'code': 500,
            'message': '开发服务器还没有这张表,无法比对，请先建表',
            'data': {}
        })
    try:
        target_data, target_field = target_sql_server.query_data(table_sql)
    except pymssql.ProgrammingError as e:
        if "Invalid object name" in str(e):
            response_data = {"code": 500, "message": "部署服务器还没有这张表,无法比对，请先建表", "data": {}}
        else:
            response_data = {"code": 500, "message": "部署服务器字段未同步，请先同步字段再刷新查看数据差异", "data": {}}
        return JsonResponse(response_data)
    except:
        traceback.print_exc()
        response_data = {"code": 500, "message": "部署服务器还没有这张表,无法比对，请先建表", "data": {}}
        return JsonResponse(response_data)
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
    # 进程安全 list
    # manager = Manager()
    shared_data_list = []
    # shared_data_list.insert(0, target_sql_server)
    for obj in source_data_list:
        process_data(obj, target_sql_server, table_name, target_identity_list, shared_data_list)
    # print("Before executor.map", shared_data_list[0])

    # with ProcessPoolExecutor() as executor:
    #     futures = [executor.submit(process_data, obj, table_name, target_identity_list, shared_data_list) for obj in source_data_list]
    # print("After executor.map", futures)

    def handel_byte_fields(source: dict):
        let_dict = {}
        for _key, _value in source.items():
            if isinstance(_value, bytes):
                _value = f"BLOB({math.ceil(len(_value) / 1024 * 100) / 100}KB)二进制无法查看"
            let_dict[_key] = _value

        return let_dict
    
    # 共享的数据列表
    source_data_list = list(map(lambda x: handel_byte_fields(x), shared_data_list))
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
    return JsonResponse(response_data)


# 通用同步
@require_http_methods(['POST'])
@login_wraps
@transaction.atomic
def auto_sync(request):
    target_sql_server = sqlserver.SqlServerObject(dbname='db2',
                                                  host=target_server['ip'],
                                                  charset='UTF-8',
                                                  database=target_server['db'],
                                                  ssh_host=target_server.get('ssh_ip'),
                                                  **target_server)  # 服务器
    json_data = json.loads(request.body)
    sql: str = json_data.get('sql')
    err_list = []
    success_list = []
    try:
        log_generate(sql_list=[sql], target_connetion=target_sql_server, REMOTE_ADDR=request.META['REMOTE_ADDR'])
        target_sql_server.insert_data(sql)
        success_list.append(sql)
    except Exception as e:
        traceback.print_exc()
        err = str(e)
        if '(1505,' in err:
            value = err[err.find("The duplicate key value is "):err.find(".DB")]
            err_list.append('该唯一索引无法创建，因为该字段有重复数据; 该表需要相关人员参与' + value)
        elif '(8111' in err:
            err_list.append('无法在表中可为 Null 的列上定义 PRIMARY KEY 约束')
        err_list.append(sql + ',')
    response_data = {
        'code': 200,
        'message': '请查看结果',
        'data': {"success": success_list, "error": err_list}
    }
    return JsonResponse(response_data)


@require_http_methods(['POST'])
def get_change_table(request):
    # TODO 拿到数据有变动的表  浪费性能弃用 后续改为多进程版本
    global source_sql_server, target_sql_server
    is_change = True
    return JsonResponse({'code': 200, 'message': '成功', 'data': is_change})


def login_out(request):
    global online_list
    try:
        online_list.remove(request.META['REMOTE_ADDR'])
    except ValueError:
        return JsonResponse({'code': 400, 'message': '你还未登录'})
    return JsonResponse({'code': 200, 'message': '退出成功'})


class DBView(ViewSet):
    def __init__(self):
        super().__init__()
        self.main_model = SysSyncTable
        self.detail_model = SysSyncTableDetail

    def get_drop_down_data(self, reqeust):
        """
        获取下拉框数据
        :param reqeust:
        :return:
        """
        source_sql_server = sqlserver.SqlServerObject(host=source_server['ip'],
                                                      database=source_server['db'],
                                                      ssh_host=source_server.get('ssh_ip'),
                                                      **source_server)  # 开发服务器
        source_sql = """
                        SELECT
                            b.t_mc,
                            a.TABLE_NAME AS table_name,
                            a.table_catalog as db
                        FROM
                            INFORMATION_SCHEMA.tables a
	                    LEFT JOIN sys_csh_bdy b ON a.table_name= b.tablename
                    """
        try:
            source_data = source_sql_server.sqlserver_data(source_sql)
        except Exception as e:
            print(e)
            source_sql = """
                           SELECT table_name  AS table_name, table_catalog as db
                            FROM information_schema.tables
                            WHERE table_type = 'BASE TABLE'
                        """
            source_data = source_sql_server.sqlserver_data(source_sql)

        return JsonResponse({'code': 200, 'message': '成功', 'data': source_data})

    def get_data(self, request: Request):
        """
        获取主模型数据
        :return:
        """
        query = self.main_model.objects.all()
        return JsonResponse({'code': 200, 'message': '成功', 'data': list(query.values())})

    def get_detail(self, request):
        """
        获取明细数据
        :return:
        """
        query = self.detail_model.objects.filter(plan_id=request.GET.get('id'))

        return JsonResponse({'code': 200, 'message': '成功', 'data': list(query.values())})

    @transaction.atomic
    def save_data(self, request):
        """
        保存方案
        :return:
        """
        main_data = request.data.get('main_data', {})
        grid_data = request.data.get('grid_data', [])
        if not all([main_data, grid_data]):
            return JsonResponse({'code': 400, 'message': '你看看你的表单', 'data': request.data})
        obj, created = self.main_model.objects.update_or_create(id=main_data.get('id', 0), defaults=main_data)
        list(map(lambda ng: self.detail_model.objects.update_or_create(id=ng.get('id'), defaults=ng),
                 list(map(lambda g: {**g, 'plan_id': obj.id}, grid_data))))
        del obj.__dict__['_state']  # 将模型对象转换为JSON
        return JsonResponse({'code': 200, 'message': '保存成功', 'data': obj.__dict__})

    @transaction.atomic
    def execl_plan(self, request):
        """
        执行方案
        :param request:
        :return:
        """
        global source_server, target_server

        if not all([source_server, target_server]):
            return JsonResponse({'code': 400, 'message': '请先连接完全数据库'})
        json_data = request.data
        if json_data.get('source'):
            login_server[request.META['REMOTE_ADDR']]['source_server'] = json_data.get('source')
        if json_data.get('target'):
            login_server[request.META['REMOTE_ADDR']]['target_server'] = json_data.get('target')

        source_server = login_server.get(request.META['REMOTE_ADDR']).get('source_server', {})
        target_server = login_server.get(request.META['REMOTE_ADDR']).get('target_server', {})
        try:
            source_sql_server = sqlserver.SqlServerObject(host=source_server['ip'],
                                                          database=source_server['db'],
                                                          ssh_host=source_server.get('ssh_ip'),
                                                          **source_server)  # 开发服务器

            target_sql_server = sqlserver.SqlServerObject(host=target_server['ip'],
                                                          charset='GBK',
                                                          database=target_server['db'],
                                                          ssh_host=target_server.get('ssh_ip'),
                                                          **target_server)  # 服务器
        except Exception:
            return JsonResponse({'code': 400, 'message': '请刷新重试'})

        try:
            main_object = self.main_model.objects.get(id=request.data.get('id'), ty=False)
        except self.main_model.DoesNotExist:
            return JsonResponse({'code': 400, 'message': '执行方案不存在或者已停用'})
        else:
            main_object.zxrq = datetime.datetime.now()
            main_object.save()

        # 同步start
        detail_query = self.detail_model.objects.filter(plan_id=request.data.get('id'))

        excel_sql_list, success_list, error_list = [], [], []
        get_data = {'source_sql_server': source_sql_server, 'target_sql_server': target_sql_server}
        for table in detail_query:
            get_data.update(table=table.table_name)
            request.GET = get_data
            response_data = table_field_compare(request)
            result = json.loads(response_data.content)
            if result.get('code') != 200:
                error_list.append({table.table_name: '字段或索引同步失败：' + result.get('message')})
                continue
            if table.sync_field:
                # 同步字段
                for t in result['data']:
                    if not t.get('sql_field'):
                        continue
                    excel_sql_list.append(t.get('sql_field'))

            if table.sync_index:
                # 同步索引
                for t in result['data']:
                    if not t.get('sql_indexes'):
                        continue
                    excel_sql_list.append(t.get('sql_indexes'))

            if table.sync_data:
                # 同步数据
                response_data = table_data_compare(request)
                result = json.loads(response_data.content)
                if result.get('code') != 200:
                    error_list.append({table.table_name: '数据同步失败：' + result.get('message')})
                    continue
                for sql in result['data']['source_data']:
                    excel_sql_list.append(sql['sql'])

        target_sql_server = sqlserver.SqlServerObject(host=target_server['ip'],
                                                      charset='UTF-8',
                                                      database=target_server['db'],
                                                      ssh_host=target_server.get('ssh_ip'),
                                                      **target_server)  # 服务器
        # 记录日志
        excel_sql_list = [element if isinstance(element, list) else [element] for element in excel_sql_list]
        excel_sql_list = [item for sublist in excel_sql_list for item in sublist]
        log_generate(request.data.get('id'), excel_sql_list, target_connetion=target_sql_server,
                     REMOTE_ADDR=request.META['REMOTE_ADDR'])
        for sql in excel_sql_list:
            try:
                target_sql_server.insert_data(sql)
                success_list.append(sql)
            except Exception as e:
                traceback.print_exc()
                err = str(e)
                if '(1505,' in err:
                    value = err[err.find("The duplicate key value is "):err.find(".DB")]
                    error_list.append('该唯一索引无法创建，因为该字段有重复数据; 该表需要相关人员参与' + value)
                elif '(8111' in err:
                    error_list.append('无法在表中可为 Null 的列上定义 PRIMARY KEY 约束')
                error_list.append(sql + ',')
        response_data = {
            'code': 200,
            'message': '请查看结果',
            'data': {"success": success_list, "error": error_list}
        }
        return JsonResponse(response_data)

    @transaction.atomic()
    def revoke_plan(self, request):
        """
        一键撤销
        :param request:
        :return:
        """
        global source_server, target_server
        json_data = request.data
        if json_data.get('source'):
            login_server[request.META['REMOTE_ADDR']]['source_server'] = json_data.get('source')
        if json_data.get('target'):
            login_server[request.META['REMOTE_ADDR']]['target_server'] = json_data.get('target')

        source_server = login_server.get(request.META['REMOTE_ADDR']).get('source_server', {})
        target_server = login_server.get(request.META['REMOTE_ADDR']).get('target_server', {})
        success_list, error_list = [], []
        recover_batch__max = SyncLogTable.objects.aggregate(Max('recover_batch'))['recover_batch__max']
        sql_list = list(SyncLogTable.objects.filter(plan_id=request.data.get('id'),
                                                    recover_batch=recover_batch__max if recover_batch__max else 0).values_list(
            'recover_sql', flat=True))
        if sql_list:
            target_sql_server = sqlserver.SqlServerObject(host=target_server['ip'],
                                                          charset='UTF-8',
                                                          database=target_server['db'],
                                                          ssh_host=target_server.get('ssh_ip'),
                                                          **target_server)  # 服务器
            for sql in sql_list:
                try:
                    target_sql_server.insert_data(sql)
                    success_list.append(sql)
                except Exception as e:
                    print(sql)
                    err = str(e)
                    print(err)
                    error_list.append(sql + ',')
        return JsonResponse(
            {'code': 200, 'message': '撤销成功, 请查看结果', 'data': {'success_list': success_list, 'error_list': error_list}})

    @transaction.atomic()
    def execl_construct_plan(self, request):
        """
        执行系统默认方案
        :param request:
        :return:
        """
        global source_server, target_server
        source_server = {
            "zd": "source",
            "ip": "172.17.18.110",
            "port": "1433",
            "user": "sa",
            "password": "CgSqlServerRoot2012",
            "db": "cgyypt"
        }
        target_server = {
                    "sshipCheckBox": True,
                    "zd": "target",
                    "ip": "172.17.0.133",
                    "port": "1433",
                    "user": "sa",
                    "password": "CgSqlServerRoot2012",
                    "db": "cgyypt",
                    "ssh_password": "ChatServerpassword321.",
                    "ssh_user": "root",
                    "ssh_port": "10022",
                    "ssh_ip": "chat.cxtech.vip"
        }
        request.data.update({'id': 0, 'source': source_server, 'target': target_server})
        if not login_server.get(request.META['REMOTE_ADDR']):
            login_server[request.META['REMOTE_ADDR']] = {'source_server': None, 'target_server': None}
        self.execl_plan(request)
        login_out(request)
        return JsonResponse(
            {'code': 200, 'message': '执行成功', 'data': None})