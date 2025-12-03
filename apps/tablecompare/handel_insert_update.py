import datetime
import traceback


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
                "{}".format(v if isinstance(v, int) else f"'{v}'") if v or v == 0 else 'null') + ','
    return set_clause


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
    temp_b = bt.hex()
    # for b in bt:
    #     temp_b += format(b, '02X')
    return '0x' + temp_b


def handel_insert_dict_to_sql(_dict: dict, column, value) -> tuple:
    for k, v in _dict.items():
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
            value += f"""{(v if isinstance(v, int) else "'{}'".format(v)) if v or v == 0 else 'null'},"""
    return column, value


def process_data(obj, target_sql_server, table_name, target_identity_list, manager_list):
    try:
        target_sql_id = f"SELECT id FROM [{table_name}] where id = %s" % obj['id']
        target_data = target_sql_server.query_data(target_sql_id)[0]  # 查找部署服务器上有重复的没 有就update  无就insert
    except Exception as e:
        print(e)
        target_data = set()
    column, value, sql = '', '', None
    if len(target_data) == 0:  # insert
        column, value = handel_insert_dict_to_sql(obj, column, value)
        if value:
            if target_identity_list:
                sql = f"SET IDENTITY_INSERT [{table_name}] ON insert into [{table_name}]({column[:len(column) - 1]}) values ({value[:len(value) - 1]}) SET IDENTITY_INSERT [{table_name}] OFF"
            else:
                sql = f"insert into [{table_name}]({column[:len(column) - 1]}) values ({value[:len(value) - 1]})"
    else:  # update
        temp_sql_str = handel_update_dict_to_sql(obj)
        if temp_sql_str:
            sql = f"update [{table_name}] set {temp_sql_str[:len(column) - 1]} where id = {obj['id']}"
    obj['sql'] = sql
    manager_list.append(obj)
