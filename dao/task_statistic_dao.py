from dao.base_dao import Dao

class TaskStatisticDao(Dao):
    def __init__(self, host, user, passwd, db, port=3306):
        super().__init__(host, user, passwd, db, port)

    def select(self):
        sql = 'select id, spider_name, date, time_cost, num_collected_item, num_succ_url, num_fail_url, raw_json from task_statistic limit 10;'
        cursor = self.conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        cursor.close()
        rt = []
        for item in data:
            rt.append({
                'id': item[0],
                'spider_name': item[1],
                'date': item[2],
                'time_cost': item[3],
                'num_collected_item': item[4],
                'num_succ_url': item[5],
                'num_fail_url': item[6],
                'raw_json': item[7]
            })
        return rt

    def select_with_spider_name_and_date(self, spider_name, date):
        sql = "select id, spider_name, date, time_cost, num_collected_item, num_succ_url, num_fail_url, raw_json from task_statistic where spider_name = '%s' and date = '%s';" % (spider_name, date)
        cursor = self.conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        cursor.close()
        if len(data) == 0:
            return None
        item = data[0]
        return {
            'id': item[0],
            'spider_name': item[1],
            'date': item[2],
            'time_cost': item[3],
            'num_collected_item': item[4],
            'num_succ_url': item[5],
            'num_fail_url': item[6],
            'raw_json': item[7]
        }

    def insert(self, obj):
        if 'spider_name' not in obj or 'date' not in obj or 'time_cost' not in obj or 'num_collected_item' not in obj or 'num_succ_url' not in obj or 'num_fail_url' not in obj or 'raw_json' not in obj:
            return 1
        sql = 'insert into task_statistic (spider_name, date, time_cost, num_collected_item, num_succ_url, num_fail_url, raw_json) values (%s, %s, %s, %s, %s, %s, %s);'
        cursor = self.conn.cursor()
        spider_name = obj['spider_name']
        date = obj['date']
        time_cost = obj['time_cost']
        num_collected_item = obj['num_collected_item']
        num_succ_url = obj['num_succ_url']
        num_fail_url = obj['num_fail_url']
        raw_json = obj['raw_json']
        cursor.execute(sql, [spider_name, date, time_cost, num_collected_item, num_succ_url, num_fail_url, raw_json])
        self.conn.commit()
        cursor.close()
        return 0

