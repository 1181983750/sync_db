

def sql_server_handle(column: list, data_list: set) -> list:
    source_data_list = []
    for data in data_list:
        data_dict = dict(zip(column, data))
        source_data_list.append(data_dict)
    return source_data_list