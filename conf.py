'''
Description: 
Version: 1.0.0
Autor: hrlu.cn
Date: 2021-07-27 15:40:04
LastEditors: hrlu.cn
LastEditTime: 2021-07-29 14:32:19
'''

CANAL_SERVER = {
    'HOSTNAME': '127.0.0.1',
    'PORT': 11111, 
    'USER': b'',
    'PASSWORD': b'',
    'CLIENT_ID': b'2001',
    'DESTINATION': b'example',
    'FILTER': b'.*\\..*',
}

MYSQL_SERVER = {
    'HOSTNAME': '127.0.0.1',
    'PORT': 3306,
    'USER': 'root',
    'PASSWORD': '',
}

DEBUG = True

FETCH_BATCH_SIZE = 1024
FETCH_FREQUENCY = 10