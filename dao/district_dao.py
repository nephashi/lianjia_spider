from dao.base_dao import Dao

# create table district ( id int unsigned primary key auto_increment, url varchar(255), name varchar(255), date varchar(64));
class DistrictDao(Dao):
    def __init__(self, host, user, passwd, db, port=3306):
        super().__init__(host, user, passwd, db, port)

    def select(self):
        sql = 'select id, url, name, superior, date from district limit 10;'
        cursor = self.conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        cursor.close()
        rt = []
        for item in data:
            rt.append({
                'id': item[0],
                'url': item[1],
                'name': item[2],
                'superior': item[3],
                'date': item[4]
            })
        return rt

    def select_with_date(self, date):
        date = date.strftime('%y-%m-%d')
        sql = "select id, url, name, superior, date from district where date='%s';" % date
        cursor = self.conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        cursor.close()
        rt = []
        for item in data:
            rt.append({
                'id': item[0],
                'url': item[1],
                'name': item[2],
                'superior': item[3],
                'date': item[4]
            })
        return rt

    def select_with_name(self, name):
        sql = "select id, url, name, superior, date from district where name='%s';" % name
        cursor = self.conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        cursor.close()
        rt = []
        for item in data:
            rt.append({
                'id': item[0],
                'url': item[1],
                'name': item[2],
                'superior': item[3],
                'date': item[4]
            })
        return rt

    # 插入一个
    # obj: dict
    def insert(self, obj):
        if 'url' not in obj or 'name' not in obj or 'superior' not in obj or 'date' not in obj:
            return 1
        sql = 'insert into district(url, name, superior, date) values (%s, %s, %s, %s);'
        cursor = self.conn.cursor()
        url = obj['url']
        name = obj['name']
        superior = obj['superior']
        date = obj['date'].strftime('%y-%m-%d')
        cursor.execute(sql, [url, name, superior, date])
        self.conn.commit()
        cursor.close()
        return 0

    # 插入一批
    # obj: list of dict
    def insert_many(self, objs):
        sql = 'insert into district(url, name, superior, date) values(%s, %s, %s, %s);'
        cursor = self.conn.cursor()
        tmpdata = []
        for obj in objs:
            if 'url' not in obj or 'name' not in obj or 'superior' not in obj or 'date' not in obj:
                return 1
            url = obj['url']
            name = obj['name']
            superior = obj['superior']
            date = obj['date'].strftime('%y-%m-%d')
            tmpdata.append([url, name, superior, date])
        if len(tmpdata) == 0:
            return 1
        cursor.executemany(sql, tmpdata)
        self.conn.commit()
        cursor.close()
        return 0

# dao = DistrictDao("127.0.0.1", "root", "qwer123456", "lianjiaspiderinfo")
# objs = [{'url': 'www.baidu.com', 'name': '昌平区', 'superior': 'beijing', 'date': '2022-9-23'}, {'url': 'www.baidu.com', 'name': '顺义区', 'superior': 'beijing', 'date': '2022-9-23'}]
# dao.insert_batch(objs)