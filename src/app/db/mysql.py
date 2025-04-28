import logging

import pymysql

from src.config import get_config


def get_mysql_connection():
    config = get_config()['db']['mysql']
    conn = pymysql.connect(
        host=config['host'],
        port=config['port'],
        user=config['user'],
        password=config['pass'],
        database=config['database'],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
    )

    return conn
