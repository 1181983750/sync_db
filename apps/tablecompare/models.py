from django.db import models


class SysSyncTable(models.Model):
    id = models.AutoField(primary_key=True)
    plan_name = models.CharField(max_length=128, blank=True, null=True, verbose_name='方案名称', )
    jlrq = models.DateTimeField(blank=True, null=True, auto_now_add=True, verbose_name='建立日期', )
    zxrq = models.DateTimeField(blank=True, null=True, verbose_name='执行日期', )
    ty = models.BooleanField(blank=True, null=True, default=False, verbose_name='是否停用', )
    bz = models.CharField(max_length=255, blank=True, null=True, verbose_name='备注', )

    class Meta:
        db_table = 'SysSyncTable'
        verbose_name = '同步数据库方案主模型22'


class SysSyncTableDetail(models.Model):
    id = models.AutoField(primary_key=True)
    plan_id = models.IntegerField(blank=True, null=True, verbose_name='方案id')
    table_name = models.CharField(max_length=128, blank=True, null=True, verbose_name='表名称', )
    sync_field = models.BooleanField(blank=True, null=True, default=False, verbose_name='是否同步字段', )
    sync_data = models.BooleanField(blank=True, null=True, default=False, verbose_name='是否同步数据', )
    sync_index = models.BooleanField(blank=True, null=True, default=False, verbose_name='是否同步索引', )
    bz = models.CharField(max_length=255, blank=True, null=True, verbose_name='备注', )

    class Meta:
        db_table = 'SysSyncTableDetail'
        verbose_name = '同步数据库方案明细'


class SyncLogTable(models.Model):
    id = models.AutoField(primary_key=True)
    table_name = models.CharField(max_length=128, blank=True, null=True, verbose_name='表名称', )
    jlrq = models.DateTimeField(auto_now_add=True, verbose_name='建立日期')
    source_data = models.CharField(max_length=255, verbose_name='源数据')
    exec_sql = models.CharField(max_length=255, verbose_name='执行的sql')
    recover_sql = models.CharField(max_length=255, verbose_name='还原的sql')
    plan_id = models.IntegerField(verbose_name='执行方案id', null=True)
    recover_batch = models.IntegerField(verbose_name='还原批次')
    czy_ip = models.CharField(max_length=30, verbose_name='操作ip')

    class Meta:
        db_table = 'SyncLogTable'
        verbose_name = '同步日志'
