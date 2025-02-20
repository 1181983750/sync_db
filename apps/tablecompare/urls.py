from django.urls import path

from . import views
from .views import DBView

urlpatterns = [
    path('select_ip/', views.selcet_ip),  # 选择测试服务器及目标服务器地址
    path('select_db/', views.selcet_db),  # 选择测试服务器及目标服务器地址
    path('auto_sync/', views.auto_sync),  # 自动同步
    path('all/', views.table_compare),  # 获取所有数据库差异表比较
    path('field/compare/', views.table_field_compare),  # 表字段比较
    path('data/compare/', views.table_data_compare),  # 表数据比较
    path('get_change_table/', views.get_change_table),
    path('login_out/', views.login_out),
    path('db/get_drop_down_data/', DBView.as_view({'get': 'get_drop_down_data'}), name='获取前端下拉框所需数据'),
    path('db/get_data/', DBView.as_view({'get': 'get_data'}), name='获取方案主模型'),
    path('db/get_detail/', DBView.as_view({'get': 'get_detail'}), name='获取方案副模型'),
    path('db/save_data/', DBView.as_view({'post': 'save_data'}), name='保持方案'),
    path('db/execl_plan/', DBView.as_view({'post': 'execl_plan'}), name='执行方案'),
    path('db/110_to_133/', DBView.as_view({'get': 'execl_construct_plan'}), name='执行方案'),

    path('db/revoke_plan/', DBView.as_view({'post': 'revoke_plan'}), name='一键撤销'),

]
