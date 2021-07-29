'''
Description: 
Version: 1.0.0
Autor: hrlu.cn
Date: 2021-07-27 15:36:01
LastEditors: hrlu.cn
LastEditTime: 2021-07-27 15:39:25
'''
import MySQLdb

from conf import *
from utils import Logger


class MySQL:
    def __init__(self, config):
        self._conn = MySQLdb.connect(
            host=config['HOSTNAME'],
            user=config['USER'],
            passwd=config['PASSWORD'],
            port=config['PORT'],
            charset='utf8',
        )
        self._cursor = None

    def _get_cursor(self, ):
        self._cursor = self._conn.cursor()

    def close(self, ):
        self._conn.close()

    def execute(self, sql, ):
        if self._cursor is None:
            self._get_cursor()
        try:
            self._cursor.execute(sql)
            self._conn.commit()
            # DEBUG and Logger.debug(sql)
        except Exception as e:
            self._conn.rollback()
            Logger.error(e, sql)
