@echo off

REM 定义路径变量
set "project_path=C:\Users\OKIU\Desktop\sql_sync_server\demo_test"
set "web_path=C:\Users\OKIU\Desktop\sql_sync_server\sql_sync_web\dist"

REM 激活虚拟环境
call workon test

REM 启动 Django 服务器
start "" python "%project_path%\manage.py" runserver 60000

REM 等待服务器启动
timeout /T 3 /NOBREAK

REM 打开网页
start "" "%web_path%\index.html"

exit
