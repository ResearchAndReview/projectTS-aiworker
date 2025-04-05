import pymysql

def get_mysql_connection():
    conn = pymysql.connect(
        host='###',
        port=0000,
        user='###',
        password='###########',
        database='####',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
    )
    return conn
