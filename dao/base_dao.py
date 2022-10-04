import pymysql
from exception.not_implement_exception import NotImplementException

class Dao:
    def __init__(self, host, user, passwd, db, port=3306):
        self.conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db, charset="utf8")

    def select(self):
        raise NotImplementException

    def insert(self, obj):
        raise NotImplementException

    def insert_many(self, objs):
        raise NotImplementException

    def close(self):
        self.conn.close()