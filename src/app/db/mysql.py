import logging

import pymysql

from src.config import get_config

sqlconn = None


def get_mysql_connection():
    global sqlconn
    if sqlconn is not None:
        return sqlconn
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
    sqlconn = conn
    return conn


def get_available_nodes():
    try:
        conn = get_mysql_connection()
        with conn.cursor() as cursor:
            sql = "SELECT * FROM Node JOIN SystemInfo ON Node.id=SystemInfo.nodeId ORDER BY Node.lastAlive DESC"
            cursor.execute(sql)

            results = cursor.fetchall()
            conn.commit()  # CONCURRENCY ISSUE, but AI Worker will be one, so no problem
            return results
    except Exception as e:
        logging.error(e)


def increase_ocr_task_size(nodeid, ocr_task_size):
    try:
        conn = get_mysql_connection()
        with conn.cursor() as cursor:
            sql = "SELECT * FROM Node WHERE id = %s"
            cursor.execute(sql, [nodeid])
            result = cursor.fetchone()

            sql = "UPDATE Node SET ocrTaskSize = %s WHERE id = %s"
            cursor.execute(sql, [ result['ocrTaskSize']+ocr_task_size, nodeid ])
            conn.commit()
    except Exception as e:
        logging.error(e)


def increase_trans_task_size(nodeid, trans_task_size):
    try:
        conn = get_mysql_connection()
        with conn.cursor() as cursor:
            sql = "SELECT * FROM Node WHERE id = %s"
            cursor.execute(sql, [nodeid])
            result = cursor.fetchone()

            sql = "UPDATE Node SET transTaskSize = %s WHERE id = %s"
            cursor.execute(sql, [ result['transTaskSize']+trans_task_size, nodeid ])
            conn.commit()
    except Exception as e:
        logging.error(e)