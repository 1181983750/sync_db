import traceback
import pymssql
from django.utils.connection import ConnectionDoesNotExist
from sshtunnel import SSHTunnelForwarder
import sqlite3
from django.conf import settings
from django.db import connections

class SqlServerObject:
    """数据库链接操作类"""

    def __init__(self, dbname, host: str, port: int, user, password, ssh_host=None, ssh_port=None, ssh_user=None,
                 ssh_password=None, database=None, charset='GBK', as_dict=False, autocommit=True, **kwargs):
        """
        :param host:  连接主机
        :param port:  数据库端口
        :param user:  用户名
        :param password: 密码
        :param database: 数据库名称
        :param charset:  字符编码默认'GBK'
        """

        try:
            local_port = int(port)
            self.dbname = dbname
            self.database = database
            self.host = host
            self.port = port
            self.user = user
            self.password = password
            if ssh_port and ssh_host:
                self.ssh_client = SSHServerObject(ssh_host, ssh_port, ssh_user, ssh_password)
                # 生成随机的本地端口 建立本地端口转发
                self.ssh_conn = self.ssh_client.connect(host, local_port)
                local_port = self.ssh_conn.local_bind_port
            # self._conn = pymssql.connect(host='127.0.0.1' if ssh_port else host, port=local_port, user=user, password=password, database=database,
            #                              charset=charset, as_dict=as_dict, autocommit=autocommit)
            try:
                self._conn = connections[dbname]
            except ConnectionDoesNotExist:
                self.set_dynamic_db()
                self._conn = connections[dbname]

            self._cursor = self._conn.cursor()
        except Exception as e:
            traceback.print_exc()
            raise RuntimeError('数据连接错误{}'.format(e))

    def set_dynamic_db(self):
        settings.DATABASES[self.dbname] = {
            'ENGINE': 'mssql',
            'NAME': self.database,
            'USER': self.user,
            'PASSWORD': self.password,
            'HOST': self.host,
            'PORT': self.port,
        }
        # 清除旧连接
        if self.dbname in connections:
            del connections[self.dbname]

    # 查询字段
    def query_table_field_sql(self, sql):
        self._cursor.execute(sql)
        # 判断是否有结果集（适配存储过程/普通查询）
        if self._cursor.description:
            result =  [tuple(table_field) for table_field in self._cursor]
            return result
        else:
            return []

    # 查询数据
    def sqlserver_data(self, sql: str, params_list=()):
        data_list = []
        if params_list:
            self._cursor.execute(sql, params=params_list)
        else:
            self._cursor.execute(sql)
        columns = [column[0] for column in self._cursor.description]
        for row in self._cursor.fetchall():
            data_list.append(dict(zip(columns, row)))
        return data_list

    def query_data(self, sql):
        self._cursor.execute(sql)
        cols = self._cursor.description  # 获取列名
        column_list = []
        for column in cols:
            column_list.append(column[0])
        result = self._cursor.fetchall()
        return set(result), column_list

    # 插入数据
    def insert_data(self, sql):
        try:
            self._cursor.execute(sql)
        except Exception as e:
            # traceback.print_exc()
            self._conn.rollback()
            raise Exception(e)

    def sync_data(self, sql, params):
        try:
            self._cursor.execute(sql, params)
        except Exception as e:
            # traceback.print_exc()
            self._conn.rollback()
            raise Exception(e)


class SSHServerObject:
    def __init__(self, ssh_host: str, ssh_port: int, ssh_user: str,
                 ssh_password: str):
        """
            SSH链接对象
        :param ssh_host:
        :param ssh_port:
        :param ssh_username:
        :param ssh_password:
        """
        # SSH 配置
        self.ssh_host = ssh_host
        self.ssh_port = int(ssh_port)
        self.ssh_user = ssh_user
        self.ssh_password = ssh_password
        self.ssh_client = None

    def connect(self, remote_address: str, remote_port: int) -> SSHTunnelForwarder:
        # 建立 SSH 隧道
        server = SSHTunnelForwarder(
                (self.ssh_host, self.ssh_port),
                ssh_username=self.ssh_user,
                ssh_password=self.ssh_password,
                remote_bind_address=(remote_address, remote_port))
        server.start()
        print('ssh连接成功！')
        return server


class LocalDatabaseManager:
    """
    本地方案存储
        示例：
    >>> with LocalDatabaseManager() as db:
    >>>     try:
    >>>         # 运行查询
    >>>         select_query = "SELECT * FROM users"
    >>>         result = db.execute_query(select_query)
    >>>         for row in result:
    >>>             print("ID:", row[0])
    >>>             print("Username:", row[1])
    >>>             print("Email:", row[2])
    >>>             print()
    >>>         # 插入数据
    >>>         insert_query = "INSERT INTO users (username, email) VALUES (?, ?)"
    >>>         new_user = ('jane_doe', 'jane@example.com')
    >>>         db.insert_data(insert_query, new_user)
    >>>         # 模拟异常
    >>>         # raise Exception("Some error occurred")  # 将此行取消注释以测试异常处理
    >>>     except Exception as e:
    >>>         print("An error occurred:", e)
    >>>         # 异常会触发 __exit__ 方法中的回滚操作
    """

    def __init__(self, db_path='mydatabase.db'):
        self.db_path = db_path

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.conn.rollback()  # 发生异常时回滚事务
        else:
            self.conn.commit()  # 没有异常时提交事务
        self.conn.close()

    def execute_query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def insert_data(self, query, data):
        self.cursor.execute(query, data)
