import pymysql
import logging

def insert(conn, obj):
    sql = 'insert into district(url, name, date) values(%s, %s, %s);'
    cursor = conn.cursor()
    url = obj['url']
    name = obj['name']
    date = obj['date']
    cursor.execute(sql, [url, name, date])
    conn.commit()
    cursor.close()

def select(conn):
    sql = 'select url, name, date from district;'
    cursor = conn.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    cursor.close()
    return data

# conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='qwer123456', db='lianjiaspiderinfo', charset="utf8")
# objs = [{'url': 'www.baidu.com', 'name': '昌平区', 'date': '2022-9-23'}, {'url': 'www.baidu.com', 'name': '顺义区', 'date': '2022-9-23'}]
# insert(conn, objs)
# logger = logging.getLogger('test')
# handler = logging.StreamHandler()
# logger.addHandler(handler)
# logger.setLevel(logging.INFO)
# logger.info('hello world!')
conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='qwer123456', db='lianjiaspiderinfo', charset="utf8")
sql = 'insert into test(info) values(%s);'
cursor = conn.cursor()
cursor.execute(sql, ['bbbbbbbb'])
conn.commit()
cursor.close()
conn.close()
