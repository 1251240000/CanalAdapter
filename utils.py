'''
Description: 
Version: 1.0.0
Autor: hrlu.cn
Date: 2021-07-27 17:46:15
LastEditors: hrlu.cn
LastEditTime: 2021-07-29 14:35:06
'''

import os
import json
import datetime

from conf import DEBUG, CANAL_SERVER, MYSQL_SERVER


class Logger:
    _log_file_name = 'canal_adapter.log'

    @classmethod
    def log(cls, text):
        with open(cls._log_file_name, 'a', encoding='utf-8') as f:
            if not text.endswith('\n'):
                text += '\n'

            f.write(text)
            DEBUG and print(text, end='')

    @classmethod
    def debug(cls, msg=''):
        cls.log(f"{datetime.datetime.now()} [DEBUG] {msg}.")

    @classmethod
    def warning(cls, msg=''):
        cls.log(f"{datetime.datetime.now()} [WARNING] {msg}.")

    @classmethod
    def error(cls, exception, msg=''):
        cls.log(f"{datetime.datetime.now()} [ERROR] {exception}, {msg}.")


class BinlogCursor:
    _cursor_file_name = 'cursor.json'
    _logfile_offset_dict = None

    _canal_server_change_reminder = "Canal server has been changed, Do you want to reset the offset of all the binlog files ? [Y/N]: "
    _mysql_server_change_reminder = "Mysql server has been changed, Do you want to reset the offset of all the binlog files ? [Y/N]: "

    def __init__(self, ):
        if not os.path.exists(self._cursor_file_name):
            self._logfile_offset_dict = {}
            self.save()
        else:
            try:
                with open(self._cursor_file_name) as f:
                    recd = json.load(f)
                
                if recd['logfile_offset_dict'] and recd['canal_server_name'] != CANAL_SERVER['HOSTNAME']:
                    if input(self._canal_server_change_reminder) == 'Y':
                        recd['logfile_offset_dict'].clear()
                if recd['logfile_offset_dict'] and recd['mysql_server_name'] != MYSQL_SERVER['HOSTNAME']:
                    if input(self._mysql_server_change_reminder) == 'Y':
                        recd['logfile_offset_dict'].clear()
                
                self._logfile_offset_dict = recd['logfile_offset_dict']
            except:
                self._logfile_offset_dict = {}
                self.save()

    def check_valid(self, logfile, pos):
        if self._logfile_offset_dict.get(logfile, 0) >= pos:
            return False
        self._logfile_offset_dict[logfile] = pos
        return True

    def save(self, ):
        with open(self._cursor_file_name, 'w') as f:
            json.dump({
                'canal_server_name': CANAL_SERVER['HOSTNAME'],
                'mysql_server_name': MYSQL_SERVER['HOSTNAME'],
                'last_updated_date': str(datetime.datetime.now()),
                'logfile_offset_dict': self._logfile_offset_dict,
            }, f)
