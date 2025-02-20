# Creation time: 2023-09-12 10:17
# The author: Tiger_YC
"""
                   _ooOoo_
                  o8888888o
                  88" . "88
                  (| -_- |)
                  O\  =  /O
               ____/`---'\____
             .'  \\|     |//  `.
            /  \\|||  :  |||//  \
           /  _||||| -:- |||||-  \
           |   | \\\  -  /// |   |
           | \_|  ''\---/''  |   |
           \  .-\__  `-`  ___/-. /
         ___`. .'  /--.--\  `. . __
      ."" '<  `.___\_<|>_/___.'  >'"".
     | | :  `- \`.;`\ _ /`;.`/ - ` : | |
     \  \ `-.   \_ __\ /__ _/   .-` /  /
======`-.____`-.___\_____/___.-`____.-'======
                   `=---='
             佛祖保佑       永无BUG
"""
import abc
from abc import ABCMeta


class SqlHandel(metaclass=ABCMeta):
    """
    sql处理类
    """

    def __new__(cls, db_type):
        """
        获取数据库类型
        :return:
        """
        if db_type == 'Mysql':
            return MySql(db_type)
        elif db_type == 'SQLServer':
            return SQLServer(db_type)

    @abc.abstractmethod
    def get_all_db_name(self):
        """
        获取数据库名称
        :return:
        """
        ...

    @abc.abstractmethod
    def get_increase_field(self, db_source_name, table_name):
        """获取自增字段"""
        ...

    def get_field_info_by_tablename(self, table_name):
        """获取表字段信息"""
        return """select COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE,NUMERIC_PRECISION, NUMERIC_SCALE from information_schema.COLUMNS where table_name = '%s'""" % table_name

    def get_primary_key_field(self, table_name):
        return """select * from information_schema.KEY_COLUMN_USAGE where table_name = '%s'""" % table_name


class SQLServer(SqlHandel):

    def get_all_db_name(self):
        """
        获取所有数据名称
        :return:
        """
        return """select name as db_name from sysdatabases where dbid>4"""

    def get_increase_field(self, db_source_name, table_name):
        """
        获取自增字段
        :return:
        """
        return """SELECT tb.name as '表名', t1.name as '字段名',case when  t4.id is null then 'false' else 'true' end as '是否主键', 
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


class MySql(SqlHandel):

    def get_all_db_name(self):
        """
        获取所有数据名称
        :return:
        """
        return """SELECT SCHEMA_NAME AS db_name
FROM information_schema.SCHEMATA
WHERE SCHEMA_NAME NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') AND SCHEMA_NAME NOT LIKE 'innodb%' AND SCHEMA_NAME NOT LIKE 'performance_schema';"""

    def get_increase_field(self, db_source_name, table_name):
        """获取自增字段"""
        return """SELECT
    tb.TABLE_NAME AS '表名',
    t1.COLUMN_NAME AS '字段名',
    CASE
        WHEN t2.CONSTRAINT_NAME IS NOT NULL THEN 'true'
        ELSE 'false'
    END AS '是否主键',
    CASE
        WHEN t1.EXTRA = 'auto_increment' THEN 'true'
        ELSE 'false'
    END AS '是否自增',
    t1.DATA_TYPE AS '类型',
    COALESCE(t1.COLUMN_COMMENT, '') AS descr
FROM
    information_schema.COLUMNS t1
LEFT JOIN
    information_schema.KEY_COLUMN_USAGE t2 ON t1.TABLE_NAME = t2.TABLE_NAME
    AND t1.COLUMN_NAME = t2.COLUMN_NAME
    AND t1.TABLE_SCHEMA = t2.TABLE_SCHEMA
LEFT JOIN
    information_schema.TABLES tb ON t1.TABLE_NAME = tb.TABLE_NAME
    AND t1.TABLE_SCHEMA = tb.TABLE_SCHEMA
WHERE
   tb.TABLE_SCHEMA = '%s' -- 指定你的数据库名称
   AND tb.TABLE_NAME = '%s'
ORDER BY
    t1.ORDINAL_POSITION;
""" % (db_source_name, table_name)

    def get_all_sql(self):
        return  """SELECT
    tb.TABLE_NAME AS '表名',
    t1.COLUMN_NAME AS '字段名',
    CASE
        WHEN t2.CONSTRAINT_NAME IS NOT NULL THEN 'true'
        ELSE 'false'
    END AS '是否主键',
    CASE
        WHEN t1.EXTRA = 'auto_increment' THEN 'true'
        ELSE 'false'
    END AS '是否自增',
    t1.DATA_TYPE AS '类型',
    COALESCE(t1.COLUMN_COMMENT, '') AS descr
FROM
    information_schema.COLUMNS t1
LEFT JOIN
    information_schema.KEY_COLUMN_USAGE t2 ON t1.TABLE_NAME = t2.TABLE_NAME
    AND t1.COLUMN_NAME = t2.COLUMN_NAME
    AND t1.TABLE_SCHEMA = t2.TABLE_SCHEMA
LEFT JOIN
    information_schema.TABLES tb ON t1.TABLE_NAME = tb.TABLE_NAME
    AND t1.TABLE_SCHEMA = tb.TABLE_SCHEMA
WHERE
   tb.TABLE_SCHEMA = '%s' -- 指定你的数据库名称
   AND tb.TABLE_NAME = '%s'
ORDER BY
    t1.ORDINAL_POSITION;
""" % (db_source_name, table_name)

class MongoDB(SqlHandel):

    def handel_query_filed(self):
        ...
