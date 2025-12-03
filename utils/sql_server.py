# import os
#
# import pymssql
#
# from utils.python_leveldb import DbCline
#
#
# def sql_server_connect():
#     conn = pymssql.connect(host='172.17.18.200', user='sa', password='Sasa123', database='cgyypt', charset='GBK')
#     # 查看连接是否成功
#     cursor = conn.cursor()
#     print(cursor)
#     sql = "select * from yy_csh_ygxx where id < 5"
#     cursor.execute(sql)
#     cols = cursor.description
#     # print(col)
#     # print('\n\n总行数：' + str(cursor.rowcount))
#     # # 用一个rs变量获取数据
#     result = cursor.fetchall()
#     # print('\n\n总行数：' + str(len(result)))
#     # print(result[1])
#     data_list = []
#     col = []
#     for i in cols:
#         col.append(i[0])
#
#     for ib in result:
#         data_dict = {}
#         for iv in col:
#             data_dict[iv] = ib[col.index(iv)]
#         data_list.append(data_dict)
#     print(len(data_list))
#     cursor.close()
#     conn.close()
#     db = DbCline('department')
#     db.put(1, data_list)
#
#
# # def demo():
# #     file_name = os.path.abspath(os.path.join(os.getcwd()))
# #     f_name = file_name + '\data' + '\\'
# #     if not os.path.exists(f_name):
# #         os.makedirs(f_name)
# #     data_path = f_name + '123'
# #     print(data_path)
#
#
# class SqlServerInsertData():
#     def __init__(self):
#         try:
#             self._conn = pymssql.connect(host='172.17.18.200', user='sa', password='Sasa123', database='cgyypt', charset='GBK')
#             self._cursor = self._conn.cursor()
#         except Exception as e:
#             print("数据库连接错误")
#
#     # 执行sql
#     def execute_sql(self, sql, dbname):
#         self._cursor.execute(sql)  # 执行sql查询数据
#
#         cols = self._cursor.description  # 获取列名
#
#         result = self._cursor.fetchall()  # 获取查询的数据
#
#         self.insert_data_leveldb(cols, result, dbname)
#
#     # 插入数据到leveldb
#     def insert_data_leveldb(self, columns, result, leveldb_name):
#         data_list = []
#         column_list = []
#         db = DbCline(leveldb_name)
#         for column in columns:
#             column_list.append(column[0])
#
#         for ib in result:
#             data_dict = {}
#             for iv in column_list:
#                 data_dict[iv] = ib[column_list.index(iv)]
#             # data_list.append(data_dict)
#             db.put(data_dict.get('id'), data_dict)
#
#     # 关闭数据库连接
#     def close_db(self):
#         self._cursor.close()
#         self._conn.close()
#
#     # 析构，关闭数据库连接
#     def __del__(self):
#         print("__del__方法被调用了")
#         self.close_db()

