import pymysql

from src.config import get_config


def get_mysql_connection():
    config = get_config()
    conn = pymysql.connect(
        host=config['db']['mysql']['host'],
        port=config['db']['mysql']['port'],
        user=config['db']['mysql']['user'],
        password=config['db']['mysql']['pass'],
        database=config['db']['mysql']['database'],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
    )
    return conn
