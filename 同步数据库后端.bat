@echo off  
E:
call workon sync_sql
start python  E:\sql_sync_server\demo_test\manage.py runserver 0.0.0.0:60000
timeout /T 3 /NOBREAK
start E:\sql_sync_server\sql_sync_web\dist\index.html
exit


