from dao.base_dao import Dao

# create table sell_info (id int unsigned primary key auto_increment, lianjia_id varchar(64),  link varchar(255), title varchar(255),
# goodhouse varchar(32), position varchar(255), detail varchar(255), follow varchar(255), tags varchar(255), area float,
# total_price_str varchar(64), total_price int unsigned, unit_price_str varchar(64), unit_price int unsigned,
# district varchar(64), superior varchar(64), date varchar(64));
class SellInfoDao(Dao):
    def __init__(self, host, user, passwd, db, port=3306):
        super().__init__(host, user, passwd, db, port)
        self.nessesary_keys = ['lianjia_id', 'link', 'title', 'goodhouse', 'position', 'detail', 'follow', 'tags', 'area',
                               'total_price_str', 'total_price', 'unit_price_str', 'unit_price', 'district', 'superior', 'date']

    def select(self):
        sql = 'select id, lianjia_id, link, title, goodhouse, position, detail, follow, tags, area, total_price_str, total_price, unit_price_str, unit_price, district, superior, date from sell_info limit 10;'
        cursor = self.conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        cursor.close()
        rt = []
        for item in data:
            rt.append({
                'id': item[0],
                'lianjia_id': item[1],
                'link': item[2],
                'title': item[3],
                'goodhouse': item[4],
                'position': item[5],
                'detail': item[6],
                'follow': item[7],
                'tags': item[8],
                'area': item[9],
                'total_price_str': item[10],
                'total_price': item[11],
                'unit_price_str': item[12],
                'unit_price': item[13],
                'district': item[14],
                'superior': item[15],
                'date': item[16]
            })
        return rt

    def select_with_date(self, date):
        date = date.strftime('%y-%m-%d')
        sql = 'select id, lianjia_id, link, title, goodhouse, position, detail, follow, tags, area, total_price_str, total_price, unit_price_str, unit_price, district, superior, date from sell_info where date="%s";' % date
        cursor = self.conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        cursor.close()
        rt = []
        for item in data:
            rt.append({
                'id': item[0],
                'lianjia_id': item[1],
                'link': item[2],
                'title': item[3],
                'goodhouse': item[4],
                'position': item[5],
                'detail': item[6],
                'follow': item[7],
                'tags': item[8],
                'area': item[9],
                'total_price_str': item[10],
                'total_price': item[11],
                'unit_price_str': item[12],
                'unit_price': item[13],
                'district': item[14],
                'superior': item[15],
                'date': item[16]
            })
        return rt

    def select_with_date_and_district(self, date, district):
        date = date.strftime('%y-%m-%d')
        sql = 'select id, lianjia_id, link, title, goodhouse, position, detail, follow, tags, area, total_price_str, total_price, unit_price_str, unit_price, district, superior, date from sell_info where date="%s" and district="%s;' % (date, district)
        cursor = self.conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        cursor.close()
        rt = []
        for item in data:
            rt.append({
                'id': item[0],
                'lianjia_id': item[1],
                'link': item[2],
                'title': item[3],
                'goodhouse': item[4],
                'position': item[5],
                'detail': item[6],
                'follow': item[7],
                'tags': item[8],
                'area': item[9],
                'total_price_str': item[10],
                'total_price': item[11],
                'unit_price_str': item[12],
                'unit_price': item[13],
                'district': item[14],
                'superior': item[15],
                'date': item[16]
            })
        return rt

    def insert(self, obj):
        for nk in self.nessesary_keys:
            if nk not in obj:
                return 1
        sql = 'insert into sell_info(lianjia_id, link, title, goodhouse, position, detail, follow, tags, area,\
                total_price_str, total_price, unit_price_str, unit_price,\
                district, superior, date) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
        cursor = self.conn.cursor()
        data_list = [obj[key] for key in self.nessesary_keys]
        for i in range(len(self.nessesary_keys)):
            if self.nessesary_keys[i] == 'date':
                data_list[i] = data_list[i].strftime('%y-%m-%d')
            if self.nessesary_keys[i] == 'goodhouse' or self.nessesary_keys[i] == 'tags':
                data_list[i] = str(data_list[i])
        cursor.execute(sql, data_list)
        self.conn.commit()
        cursor.close()
        return 0

    def insert_many(self, objs):
        sql = 'insert into sell_info(lianjia_id, link, title, goodhouse, position, detail, follow, tags, area,\
                total_price_str, total_price, unit_price_str, unit_price, \
                district, superior, date) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
        cursor = self.conn.cursor()
        tmpdata = []
        for obj in objs:
            for nk in self.nessesary_keys:
                if nk not in obj:
                    return 1
            data_list = [obj[key] for key in self.nessesary_keys]
            for i in range(len(self.nessesary_keys)):
                if self.nessesary_keys[i] == 'date':
                    data_list[i] = data_list[i].strftime('%y-%m-%d')
                if self.nessesary_keys[i] == 'goodhouse' or self.nessesary_keys[i] == 'tags':
                    data_list[i] = str(data_list[i])

            tmpdata.append(data_list)
        if len(tmpdata) == 0:
            return 1
        cursor.executemany(sql, tmpdata)
        self.conn.commit()
        cursor.close()
        return 0