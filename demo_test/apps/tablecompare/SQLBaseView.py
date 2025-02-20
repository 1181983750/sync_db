# # Creation time: 2023-08-02 11:04
# # The author: Tiger_YC
# """
#                    _ooOoo_
#                   o8888888o
#                   88" . "88
#                   (| -_- |)
#                   O\  =  /O
#                ____/`---'\____
#              .'  \\|     |//  `.
#             /  \\|||  :  |||//  \
#            /  _||||| -:- |||||-  \
#            |   | \\\  -  /// |   |
#            | \_|  ''\---/''  |   |
#            \  .-\__  `-`  ___/-. /
#          ___`. .'  /--.--\  `. . __
#       ."" '<  `.___\_<|>_/___.'  >'"".
#      | | :  `- \`.;`\ _ /`;.`/ - ` : | |
#      \  \ `-.   \_ __\ /__ _/   .-` /  /
# ======`-.____`-.___\_____/___.-`____.-'======
#                    `=---='
#              佛祖保佑       永无BUG
# """
# import json
# import threading
#
# from django.http import JsonResponse
# from rest_framework.viewsets import ViewSet
#
# from apps.tablecompare.util import sqlserver
#
#
# class MainData:
#     """
#     返回主要信息
#     """
#
#     # IDENTITY(1, 1)
#     # PRIMARY
#     # KEY
#     # threading.Lock()
#     def __init__(self, table_name: Union[str] = 'TAB', action: str = 'ADD', fields_info: Union[dict, list] = []):
#         global source_server, target_server
#         self.__main_sql = ''
#         if action == 'CREATE':
#             info = ""
#             for i in fields_info:
#                 i['column_name'] = f"[{i['column_name']}]"
#                 if i['primary_key']:
#                     info += (i['column_name'] + ' ' + i['data_type'] + ' NOT NULL PRIMARY KEY IDENTITY(1,1),') if i[
#                         'identity_sql'] else (i['column_name'] + ' ' + i['data_type'] + ' NOT NULL PRIMARY KEY,')
#                 elif i['data_type'] == 'varchar' and i['max_lenght'] != '-1':
#                     info += (i['column_name'] + ' ' + i['data_type'] + '(' + i['max_lenght'] + ')' + ' NULL,') if i[
#                                                                                                                       'is_null'] == 'YES' else (
#                             i['column_name'] + ' ' + i['data_type'] + '(' + i['max_lenght'] + ')' + ' NOT NULL,')
#                 elif (i['data_type'] == 'nvarchar' or i['data_type'] == 'varchar') and i['max_lenght'] == '-1':
#                     info += (i['column_name'] + ' ' + i['data_type'] + '(MAX)' + ' NULL,') if i[
#                                                                                                   'is_null'] == 'YES' else (
#                             i['column_name'] + ' ' + i['data_type'] + '(MAX)' + ' NOT NULL,')
#                 elif i['data_type'] == 'nchar' and i['max_lenght'] != '-1':
#                     info += (i['column_name'] + ' ' + i['data_type'] + '(' + i['max_lenght'] + ')' + ' NULL,') if i[
#                                                                                                                       'is_null'] == 'YES' else (
#                             i['column_name'] + ' ' + i['data_type'] + '(' + i['max_lenght'] + ')' + ' NOT NULL,')
#                 elif (i['data_type'] == 'nvarchar' or i['data_type'] == 'char') and i['max_lenght'] != '-1':
#                     info += (i['column_name'] + ' ' + i['data_type'] + '(' + i['max_lenght'] + ')' + ' NULL,') if i[
#                                                                                                                       'is_null'] == 'YES' else (
#                             i['column_name'] + ' ' + i['data_type'] + '(' + i['max_lenght'] + ')' + ' NOT NULL,')
#                 elif i['data_type'] == 'date' or i['data_type'] == 'datetime' or i['data_type'] == 'datetime2' or i[
#                     'data_type'] == 'time':
#                     info += (i['column_name'] + ' ' + i['data_type'] + ' NULL,') if i['is_null'] == 'YES' else (
#                             i['column_name'] + ' ' + i['data_type'] + ' NOT NULL,')
#                 elif i['data_type'] == 'int' or i['data_type'] == 'bigint' or i['data_type'] == 'bit' or i[
#                     'data_type'] == 'tinyint' or i['data_type'] == 'smallint':
#                     info += (i['column_name'] + ' ' + i['data_type'] + ' NULL,') if i['is_null'] == 'YES' else (
#                             i['column_name'] + ' ' + i['data_type'] + ' NOT NULL,')
#                 elif i['data_type'] == 'decimal':
#                     info += (i['column_name'] + ' ' + i['data_type'] + '(' + str(i['num_max']) + ',' + str(
#                         i['num_min']) + ')' + ' NULL,') if i['is_null'] == 'YES' else (
#                             i['column_name'] + ' ' + i['data_type'] + '(' + str(i['num_max']) + ',' + str(
#                         i['num_min']) + ')' + ' NOT NULL,')
#                 elif i['data_type'] == 'money':
#                     info += (i['column_name'] + ' ' + i['data_type'] + ' NULL,') if i['is_null'] == 'YES' else (
#                             i['column_name'] + ' ' + i['data_type'] + ' NOT NULL,')
#                 elif i['data_type'] == "text":
#                     info += (i['column_name'] + ' ' + i['data_type'] + ' NULL,') if i['is_null'] == 'YES' else (
#                             i['column_name'] + ' ' + i['data_type'] + ' NOT NULL,')
#                 elif i['data_type'] == "varbinary" and i['max_lenght'] == "-1":
#                     info += (i['column_name'] + ' ' + i['data_type'] + '(MAX)' + ' NULL,') if i[
#                                                                                                   'is_null'] == 'YES' else (
#                             i['column_name'] + ' ' + i['data_type'] + '(MAX)' + ' NOT NULL,')
#                 elif i['data_type'] == "varbinary" and i['max_lenght'] != "-1":
#                     info += (i['column_name'] + ' ' + i['data_type'] + '(' + i['max_lenght'] + ')' + ' NULL,') if i[
#                                                                                                                       'is_null'] == 'YES' else (
#                             i['column_name'] + ' ' + i['data_type'] + '(' + i['max_lenght'] + ')' + ' NOT NULL,')
#                 else:
#                     print(i)
#
#             self.__main_sql = """CREATE TABLE %s(%s)""" % (table_name, info)
#         else:
#             fields_info['column_name'] = f"[{fields_info['column_name']}]"
#             if fields_info['data_type'] == 'text':
#                 if action == 'ADD':
#
#                     self.__main_sql = ("""ALTER TABLE [%s] ADD %s NULL""" % (table_name,
#                                                                              fields_info['column_name'] + ' ' +
#                                                                              fields_info['data_type']))
#                 elif action == 'ALTER':
#                     self.__main_sql = f"""ALTER TABLE [%s] ALTER column %s NULL """ % (table_name, fields_info.get(
#                         'column_name') + ' ' + fields_info['data_type'])
#
#             elif fields_info['max_lenght'] and fields_info['num_max'] is None and fields_info['max_lenght'] != '-1':
#                 if action == 'ADD':
#                     self.__main_sql = """ALTER TABLE [%s] ADD %s NULL""" % (table_name,
#                                                                             fields_info['column_name'] + ' ' +
#                                                                             fields_info['data_type'] + '(' + (
#                                                                                 fields_info['max_lenght']) + ')')
#                 elif action == 'ALTER':
#                     self.__main_sql = """ALTER TABLE [%s] ALTER column %s NULL""" % (table_name, fields_info.get(
#                         'column_name') + ' ' + fields_info['data_type'] + '(' + (fields_info['max_lenght']) + ')')
#             elif fields_info['max_lenght'] is None and (
#                     fields_info['data_type'] == 'int' or fields_info['data_type'] == 'tinyint' or fields_info[
#                 'data_type'] == 'bigint'):  # int类
#                 if action == 'ADD':
#                     self.__main_sql = """ALTER TABLE [%s] ADD %s NULL""" % (
#                         table_name, fields_info['column_name'] + ' ' + fields_info['data_type'])
#                 elif action == 'ALTER':
#                     self.__main_sql = """ALTER TABLE [%s] ALTER column %s NULL""" % (
#                         table_name, fields_info.get('column_name') + ' ' + fields_info['data_type'])
#             elif fields_info['data_type'] == 'decimal':  # 金钱类
#                 if action == 'ADD':
#                     self.__main_sql = """ALTER TABLE [%s] ADD %s NULL""" % (table_name,
#                                                                             fields_info.get('column_name') + ' ' +
#                                                                             fields_info['data_type'] + '(' + str(
#                                                                                 fields_info['num_max']) + ',' + str(
#                                                                                 fields_info['num_min']) + ')')
#                 elif action == 'ALTER':
#                     self.__main_sql = """ALTER TABLE [%s] ALTER column %s NULL""" % (table_name, fields_info.get(
#                         'column_name') + ' ' + fields_info['data_type'] + '(' + str(fields_info['num_max']) + ',' + str(
#                         fields_info['num_min']) + ')')
#             elif fields_info['data_type'] == 'money':
#                 if action == 'ADD':
#                     self.__main_sql = """ALTER TABLE [%s] ADD %s NULL""" % (table_name,
#                                                                             fields_info.get('column_name') + ' ' +
#                                                                             fields_info['data_type'])
#                 elif action == 'ALTER':
#                     self.__main_sql = """ALTER TABLE [%s] ALTER column %s NULL""" % (table_name, fields_info.get(
#                         'column_name') + ' ' + fields_info['data_type'])
#             elif fields_info['data_type'] == 'date' or fields_info['data_type'] == 'datetime' or fields_info[
#                 'data_type'] == 'datetime2' or fields_info[
#                 'data_type'] == 'time':
#                 if action == 'ADD':
#                     self.__main_sql = """ALTER TABLE [%s] ADD %s NULL""" % (
#                         table_name, fields_info.get('column_name') + ' ' + fields_info['data_type'])
#                 elif action == 'ALTER':
#                     self.__main_sql = """ALTER TABLE [%s] ALTER column %s NULL""" % (
#                         table_name, fields_info.get('column_name') + ' ' + fields_info['data_type'])
#             elif fields_info['data_type'] == 'bit':
#                 if action == 'ADD':
#                     self.__main_sql = """ALTER TABLE [%s] ADD %s NULL""" % (
#                         table_name, fields_info.get('column_name') + ' ' + fields_info['data_type'])
#                 elif action == 'ALTER':
#                     self.__main_sql = """ALTER TABLE [%s] ALTER column %s NULL""" % (
#                         table_name, fields_info.get('column_name') + ' ' + fields_info['data_type'])
#             elif fields_info['data_type'] == "varbinary" and fields_info['max_lenght'] == "-1":
#                 if action == 'ADD':
#                     self.__main_sql = """ALTER TABLE [%s] ADD %s NULL""" % (
#                         table_name, fields_info.get('column_name') + ' ' + fields_info['data_type'] + '(MAX)')
#                 elif action == 'ALTER':
#                     self.__main_sql = """ALTER TABLE [%s] ALTER column %s NULL""" % (
#                         table_name, fields_info.get('column_name') + ' ' + fields_info['data_type'] + '(MAX)')
#
#             elif fields_info['data_type'] == "varbinary" and fields_info['max_lenght'] != "-1":
#                 if action == 'ADD':
#                     self.__main_sql = """ALTER TABLE [%s] ADD %s NULL""" % (table_name,
#                                                                             fields_info['column_name'] + ' ' +
#                                                                             fields_info['data_type'] + '(' + (
#                                                                                 fields_info['max_lenght']) + ')')
#                 elif action == 'ALTER':
#                     self.__main_sql = """ALTER TABLE [%s] ALTER column %s NULL""" % (table_name, fields_info.get(
#                         'column_name') + ' ' + fields_info['data_type'] + '(' + (fields_info['max_lenght']) + ')')
#             if action == 'ALTER':
#                 self.__main_sql += "  " + self.__main_sql.replace('NULL', 'NULL' if fields_info[
#                                                                                         'is_null'] == 'YES' else 'NOT NULL')
#
#     def __call__(self, *args, **kwargs):
#         return self.__main_sql
#
#
# class SQLBaseView(ViewSet):
#     """
#     sql同步 基础视图
#     """
#     r_lock = threading.RLock()
#
#     def __new__(cls, *args, **kwargs):
#         if not hasattr(cls, '_instance'):
#             with cls.r_lock:
#                 _instance = super(__class__, cls).__new__(cls)
#                 setattr(cls, '_instance', _instance)
#         return getattr(cls, '_instance')
#
#     def __init__(self):
#         super().__init__()
#         self.source_server = {"ip": "172.17.18.110", "port": 1433, "user": "sa", "password": "CgSqlServerRoot2012", "db": "cgyypt"}
#         self.target_server = {"ip": "172.17.18.110", "port": 1433, "user": "sa", "password": "CgSqlServerRoot2012", "db": "cgyypt_2"}
#         # noinspection PyUnresolvedReferences
#         self.jiaoben = """CREATE PROC p_helpindex
#     (
# 		@tbname sysname = '' ,
#     @CLUSTERED INT = '1'
# 		)
# AS
#
#
#     IF @tbname IS NULL
#         OR @tbname = ''
#         RETURN -1;
#
#
#
#     DECLARE @t TABLE
#         (
#           table_name NVARCHAR(100) ,
#           schema_name NVARCHAR(100) ,
#           fill_factor INT ,
#           is_padded INT ,
#           ix_name NVARCHAR(100) ,
#           type INT ,
#           keyno INT ,
#           column_name NVARCHAR(200) ,
#           cluster VARCHAR(20) ,
#           ignore_dupkey VARCHAR(20) ,
#           [unique] VARCHAR(20) ,
#           groupfile VARCHAR(10)
#         );
#
#     DECLARE @table_name NVARCHAR(100) ,
#         @schema_name NVARCHAR(100) ,
#         @fill_factor INT ,
#         @is_padded INT ,
#         @ix_name NVARCHAR(100) ,
#         @ix_name_old NVARCHAR(100) ,
#         @type INT ,
#         @keyno INT ,
#         @column_name NVARCHAR(100) ,
#         @cluster VARCHAR(20) ,
#         @ignore_dupkey VARCHAR(20) ,
#         @unique VARCHAR(20) ,
#         @groupfile VARCHAR(10);
#
#     DECLARE ms_crs_ind CURSOR LOCAL STATIC
#     FOR
#         SELECT
#
# DISTINCT        table_name = a.name ,
#                 schema_name = b.name ,
#                 fill_factor = c.OrigFillFactor ,
#                 is_padded = CASE WHEN c.status = 256 THEN 1
#                                  ELSE 0
#                             END ,
#                 ix_name = c.name ,
#                 type = c.indid ,
#                 d.keyno ,
#                 column_name = e.name
#                 + CASE WHEN INDEXKEY_PROPERTY(a.id, c.indid, d.keyno,
#                                               'isdescending') = 1
#                        THEN ' desc '
#                        ELSE ''
#                   END ,
#                 CASE WHEN ( c.status & 16 ) <> 0 THEN 'clustered'
#                      ELSE 'nonclustered'
#                 END ,
#                 CASE WHEN ( c.status & 1 ) <> 0 THEN 'IGNORE_DUP_KEY'
#                      ELSE ''
#                 END ,
#                 CASE WHEN ( c.status & 2 ) <> 0 THEN 'unique'
#                      ELSE ''
#                 END ,
#                 g.groupname
#         FROM    sysobjects a
#                 INNER JOIN sysusers b ON a.uid = b.uid
#                 INNER JOIN sysindexes c ON a.id = c.id
#                 INNER JOIN sysindexkeys d ON a.id = d.id
#                                              AND c.indid = d.indid
#                 INNER JOIN syscolumns e ON a.id = e.id
#                                            AND d.colid = e.colid
#                 INNER JOIN sysfilegroups g ON g.groupid = c.groupid
#                 LEFT JOIN master.dbo.spt_values f ON f.number = c.status
#                                                      AND f.type = 'I'
#         WHERE   a.id = OBJECT_ID(@tbname)
#                 AND c.indid < 255
#                 AND ( c.status & 64 ) = 0
#                 AND c.indid >= @CLUSTERED
#         ORDER BY c.indid ,
#                 d.keyno;
#
#
#     OPEN ms_crs_ind;
#
#     FETCH ms_crs_ind INTO @table_name, @schema_name, @fill_factor, @is_padded,
#         @ix_name, @type, @keyno, @column_name, @cluster, @ignore_dupkey,
#         @unique, @groupfile;
#
#
#     IF @@fetch_status < 0
#         BEGIN
#
#             DEALLOCATE ms_crs_ind;
#
#             RAISERROR(15472,-1,-1);
#
#             RETURN -1;
#
#         END;
#
#     WHILE @@fetch_status >= 0
#         BEGIN
#
#             IF EXISTS ( SELECT  1
#                         FROM    @t
#                         WHERE   ix_name = @ix_name )
#                 UPDATE  @t
#                 SET     column_name = column_name + ',' + @column_name
#                 WHERE   ix_name = @ix_name;
#
#             ELSE
#                 INSERT  INTO @t
#                         SELECT  @table_name ,
#                                 @schema_name ,
#                                 @fill_factor ,
#                                 @is_padded ,
#                                 @ix_name ,
#                                 @type ,
#                                 @keyno ,
#                                 @column_name ,
#                                 @cluster ,
#                                 @ignore_dupkey ,
#                                 @unique ,
#                                 @groupfile;
#
#             FETCH ms_crs_ind INTO @table_name, @schema_name, @fill_factor,
#                 @is_padded, @ix_name, @type, @keyno, @column_name, @cluster,
#                 @ignore_dupkey, @unique, @groupfile;
#
#
#         END;
#
#     DEALLOCATE ms_crs_ind;
#
#
#     SELECT  'CREATE ' + UPPER([unique]) + CASE WHEN [unique] = '' THEN ''
#                                                ELSE ' '
#                                           END + UPPER(cluster) + ' INDEX '
#             + ix_name + ' ON ' + table_name + '(' + column_name + ')'
#             + CASE WHEN fill_factor > 0
#                         OR is_padded = 1
#                         OR ( UPPER(cluster) != 'NONCLUSTERED'
#                              AND ignore_dupkey = 'IGNORE_DUP_KEY'
#                            )
#                    THEN ' WITH ' + CASE WHEN is_padded = 1 THEN 'PAD_INDEX,'
#                                         ELSE ''
#                                    END
#                         + CASE WHEN fill_factor > 0
#                                THEN 'FILLFACTOR =' + LTRIM(fill_factor)
#                                ELSE ''
#                           END
#                         + CASE WHEN ignore_dupkey = 'IGNORE_DUP_KEY'
#                                     AND UPPER(cluster) = 'NONCLUSTERED'
#                                THEN CASE WHEN ( fill_factor > 0
#                                                 OR is_padded = 1
#                                               ) THEN ',IGNORE_DUP_KEY'
#                                          ELSE ',IGNORE_DUP_KEY'
#                                     END
#                                ELSE ''
#                           END
#                    ELSE ''
#               END + ' ON [' + groupfile + ']' AS col,Index_name=ix_name,index_keys=column_name
#     FROM    @t;
#     RETURN 0;"""
#
#     def handel_bytes_to_ten_six(self, bt: bytes):
#         """
#         二进制转十六进制
#         '02X' 是格式规范，它指定了输出的格式。在这个格式规范中：
#         0：表示使用零进行填充。
#         2：表示输出的宽度为两位。
#         X：表示将值转换为大写的十六进制表示形式。
#         d：将值格式化为十进制整数。
#         f：将值格式化为浮点数。
#         s：将值格式化为字符串。
#         x：将值格式化为小写的十六进制数。
#         X：将值格式化为大写的十六进制数。
#         b：将值格式化为二进制数。
#         o：将值格式化为八进制数。
#         e：将值格式化为科学计数法表示的浮点数。
#         ord：将Ascii码转为Unicode码
#         chr: 将Unicode码转为Ascii码
#         """
#         temp_b = ''.join(format(b, '02x') for b in bt)
#         # for b in bt:
#         #     temp_b += format(b, '02X')
#         return '0x' + temp_b
#
#     def selcet_ip(self, request):
#         """
#         连接ip 查询数据库列表
#         """
#
#         sql = """select * from sysdatabases where dbid>4"""
#         global source_server, target_server
#         json_data = json.loads(request.body)
#         s_db_list = []
#         t_db_list = []
#         if json_data.get('source') and json_data.get('source').get('ip'):
#             try:
#                 source_sql_server: sqlserver.SqlServerObject = sqlserver.SqlServerObject(host=json_data.get(
#                     'source')['ip'],
#                                                                                          port=json_data.get(
#                                                                                              'source')['port'],
#                                                                                          user=json_data.get(
#                                                                                              'source')['user'],
#                                                                                          password=json_data.get(
#                                                                                              'source')['password'],
#                                                                                          database="master")  # 开发服务器
#                 s_db_name_set = source_sql_server.query_data(sql)[0]
#                 s_db_list = [obj[0] for obj in s_db_name_set]
#                 source_server = json_data.get('source')
#             except:
#                 return JsonResponse({"code": 400, "message": "数据库连接失败", "data": {}})
#         if json_data.get('target') and json_data.get('target').get('ip'):
#             try:
#                 target_sql_server: sqlserver.SqlServerObject = sqlserver.SqlServerObject(host=json_data.get(
#                     'target')['ip'],
#                                                                                          port=json_data.get(
#                                                                                              'target')['port'],
#                                                                                          user=json_data.get(
#                                                                                              'target')['user'],
#                                                                                          password=json_data.get(
#                                                                                              'target')['password'],
#                                                                                          database="master")  # 服务器
#                 t_db_name_set = target_sql_server.query_data(sql)[0]
#                 t_db_list = [obj[0] for obj in t_db_name_set]
#                 target_server = json_data.get('target')
#             except:
#                 return JsonResponse({"code": 400, "message": "数据库连接失败", "data": {}})
#         if s_db_list or t_db_list:
#             return JsonResponse(
#                 {"code": 200, "message": "查询数据库列表成功", "data": {"source_db": s_db_list, "target_db": t_db_list}})
#         else:
#             return JsonResponse(
#                 {"code": 400, "message": "查询数据库列表失败", "data": {"source_db": s_db_list, "target_db": t_db_list}})
