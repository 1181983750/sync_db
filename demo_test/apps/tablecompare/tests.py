import os
import socket
import sys
import threading
import time

import paramiko

import pymssql
import pyodbc as pyodbc

# 查找一个可用的本地端口号
def find_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('127.0.0.1', 0))
    _, port = s.getsockname()
    s.close()
    return port


if __name__ == "__main__":
    sshClient = paramiko.SSHClient()
    sshClient.load_system_host_keys()
    sshClient.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        sshClient.connect(hostname='chat.cxtech.vip', port=10022, username='root', password='ChatServerpassword321.')

        # 打印输出和错误信息
        print("SSH连接成功")

        transport = sshClient.get_transport()
        local_port = find_free_port()
        ssh_channel = transport.open_channel(
            kind="direct-tcpip", dest_addr=('172.17.0.133', 1433), src_addr=("127.0.0.1", local_port)
        )
        try:
            # 连接到SQL Server数据库
            # conn = pymssql.connect(host='172.17.0.133', port=1433, user='sa', password='CgSqlServerRoot2012',
            #                        database='cgyypt')
            conn = pyodbc.connect(
                f'DRIVER=SQL Server Native Client 11.0;SERVER=172.17.0.133,{1433};DATABASE=cgyypt;UID=sa;PWD=CgSqlServerRoot2012'
            )
            # conn = pyodbc.connect(host='172.17.0.133', port=1433, user='sa', password='CgSqlServerRoot2012',
            #                       database='cgyypt', charset="GBK", driver='SQL Server Native Client 11.0')

            cursor = conn.cursor()
            cursor.execute("SELECT top 10 * FROM rs_ygxx")
            results = cursor.fetchall()
            for row in results:
                print(row)

            conn.close()
            print("数据库连接成功")

        except Exception as e:
            print(f"数据库连接或操作发生错误：{str(e)}")

    except Exception as e:
        print(f"SSH连接发生错误：{str(e)}")
    finally:
        sshClient.close()
