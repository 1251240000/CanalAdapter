'''
Description: 
Version: 1.0.0
Autor: hrlu.cn
Date: 2021-07-23 16:28:18
LastEditors: hrlu.cn
LastEditTime: 2021-07-23 18:14:12
'''
import time
import signal

from canal.client import Client
from canal.protocol import EntryProtocol_pb2

from database import MySQL
from utils import Logger, BinlogCursor
from conf import FETCH_FREQUENCY, FETCH_BATCH_SIZE, DEBUG


def _get_fixed_key(column):
    # 嵌套反引号以避免列名与关键词相同时产生的语法错误
    return f"`{column.name}`"


def _get_fixed_value(column):
    # 列值需对 null 单独进行处理
    if column.isNull:
        return 'null'
    # 替换为双单引号以避免出现双引号嵌套
    col_val = column.value.replace('"', "''")
    return f'"{col_val}"'


def _get_condition(row):
    # 寻找主键列，将其作为唯一条件
    if any(col.isKey for col in row.beforeColumns):
        for col in row.beforeColumns:
            if col.isKey:
                return f'{_get_fixed_key(col)} = {_get_fixed_value(col)}'
    # 否则将原各列值作为条件
    return ' and '.join(
        f'{_get_fixed_key(col)} = {_get_fixed_value(col)}'
        for col in row.beforeColumns
    )


def _get_update_fields(row):
    return ', '.join(
        f'{_get_fixed_key(col)} = {_get_fixed_value(col)}'
        for col in row.afterColumns if col.updated
    )


class CanalAdapter():

    def __init__(self, config, db=None):
        self._client = None
        self._client_config = config
        self._connect()

        self._bin_cur = BinlogCursor()

        if db is not None:
            self._db = db
            self._callback = db.execute
        else:
            self._callback = print

    def _signal_register(self, ):
        # 注册系统中断信号，以在程序退出前执行清理任务
        signal.signal(signal.SIGINT, self._clean)
        signal.signal(signal.SIGTERM, self._clean)

    def _connect(self, retry_count=0):
        if self._client is None:
            self._client = Client()

        config = self._client_config
        try:
            self._client.connect(host=config['HOSTNAME'], port=config['PORT'])
            self._client.check_valid(
                username=config['USER'], password=config['PASSWORD'])
            self._client.subscribe(
                client_id=config['CLIENT_ID'], destination=config['DESTINATION'], filter=config['FILTER'])
        except OSError as e:
            Logger.error(
                e, "Canal connection was interrupted, retry count: %d" % retry_count)
            time.sleep(10 if retry_count < 6 else 300)
            self._connect(retry_count + 1)

    def _fetch(self, ):
        try:
            return self._client.get(FETCH_BATCH_SIZE)
        except (ConnectionResetError, OSError):
            self._connect()
            return self._fetch()

    def _clean(self, *args):
        self._client.disconnect()
        self._db and self._db.close()
        self._bin_cur.save()
        DEBUG and Logger.debug('Exit safely')
        exit()

    def _process(self, ):
        message = self._fetch()
        entries = message['entries']

        for entry in entries:
            # 排除事务
            if entry.entryType in [
                EntryProtocol_pb2.EntryType.TRANSACTIONBEGIN,
                EntryProtocol_pb2.EntryType.TRANSACTIONEND,
            ]:
                continue
            # 获取本次操作的数据库及表
            database, table = entry.header.schemaName, entry.header.tableName

            logfile, offset = entry.header.logfileName, entry.header.logfileOffset
            # 对当前 Binlog 位置校验，丢弃历史数据
            if not self._bin_cur.check_valid(logfile, offset):
                DEBUG and Logger.warning('The entry at %s.%d has been discarded' % (logfile, offset))
                continue

            row_change = EntryProtocol_pb2.RowChange()
            row_change.MergeFromString(entry.storeValue)
            event = row_change.eventType

            # DDL 操作可直接获取原始 SQL 语句
            if row_change.isDdl:
                if event != EntryProtocol_pb2.EventType.QUERY:
                    self._callback(f'USE {database};')
                self._callback(f'{row_change.sql};')
                DEBUG and Logger.debug(
                    f'DDL, Db: {database}, table: {table}, event: {event}, SQL: {row_change.sql}')
            # DML 操作需手动进行转换
            else:
                for row in row_change.rowDatas:
                    # 生成 Insert 语句
                    if event == EntryProtocol_pb2.EventType.INSERT:
                        keys = '(%s)' % ', '.join(_get_fixed_key(col) for col in row.afterColumns)
                        values = '(%s)' % ', '.join(_get_fixed_value(col) for col in row.afterColumns)
                        sql = f"insert into {database}.{table} {keys} values {values};"
                    # 生成 Update 语句
                    elif event == EntryProtocol_pb2.EventType.UPDATE:
                        update_fields = _get_update_fields(row)
                        update_condition = _get_condition(row)
                        sql = f"update {database}.{table} set {update_fields} where {update_condition};"
                    # 生成 Delete 语句
                    elif event == EntryProtocol_pb2.EventType.DELETE:
                        delete_condition = _get_condition(row)
                        sql = f"delete from {database}.{table} where {delete_condition};"
                    else:
                        continue

                    DEBUG and Logger.debug(
                        f'DML, Db: {database}, table: {table}, event: {event}, SQL: {sql}')
                    self._callback(sql)

    def start(self, ):
        self._signal_register()
        while True:
            try:
                self._process()
            except Exception as e:
                Logger.error(e, )
            time.sleep(1 / FETCH_FREQUENCY)


if __name__ == '__main__':
    '''
    已知缺陷：
    1. Master 创建视图时，无法解析语句
    '''

    from conf import CANAL_SERVER, MYSQL_SERVER
    mysql = MySQL(MYSQL_SERVER)
    handler = CanalAdapter(CANAL_SERVER, db=mysql)
    handler.start()
