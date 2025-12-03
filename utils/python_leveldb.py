import datetime
# import decimal
# import json
# import os
#
# import plyvel
#
#
# class DbCline():
#     # 初始化打开数据库
#     def __init__(self, datalocation):
#         file_name = os.path.abspath(os.path.join(os.getcwd()))
#         f_name = file_name + '\data' + '\\'
#         if not os.path.exists(f_name):
#             os.makedirs(f_name)
#         data_path = f_name + datalocation
#         self._db = plyvel.DB(data_path, create_if_missing=True)
#         print('open db location:' + data_path)
#
#     # 插入数据&修改数据
#     # leveldb存储的是二进制数据，所以参数得先确保转换成字符串类型，然后在转换成二进制数据
#     # 若插入相同的key值则变为修改
#     def put(self, key, value):
#         # self._db.put(str(key).encode(), str(value).encode())
#         print(key)
#         self._db.put(str(key).encode(), json.dumps(value, default=str).encode())
#
#     # 根据Key查询Value
#     def get(self, key):
#         # 返回的是bytes型数据
#         value_buyes = self._db.get(str(key).encode())
#         # 解码后返回
#         # return value_buyes.decode()
#         return json.loads(value_buyes)
#
#     # 查询
#     def query_all(self):
#         # data_list = {}
#         # for key, value in self._db:xxx
#         #     data_list[key.decode()] = json.loads(value)
#         # return data_list
#
#         data_list = []
#         for key, value in self._db:
#             data_list.append(json.loads(value))
#         return data_list
#
#     # 根据key删除值
#     def delete(self, key):
#         self._db.delete(str(key).encode())
#
#     #关闭db
#     def close(self):
#         self._db.close()
#         print("db closed")
#
#     # 析构，关闭数据库连接
#     def __del__(self):
#         self.close()