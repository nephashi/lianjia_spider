class DBCONFIG:
    HOSTNAME = '127.0.0.1'
    PORT = 3306
    USERNAME = 'root'
    PASSWD = ''
    DBNAME = 'lianjia_spider'

class CRAWLCONFIG:
    RETRY_TIMES = 5
    # 爬取间等待时间
    INTERVAL_TIME_SECOND = 1
    # 爬取url去重
    SKIP_DUPLICATE_URL = False

DBCONFIG_FNAME = ''
if len(DBCONFIG_FNAME) > 0:
    import json
    import logging
    with open(DBCONFIG_FNAME) as f:
        line = f.readline()
        obj = json.loads(line)
        DBCONFIG.HOSTNAME = obj['hostname']
        DBCONFIG.PORT = obj['port']
        DBCONFIG.USERNAME = obj['username']
        DBCONFIG.PASSWD = obj['passwd']
        DBCONFIG.DBNAME = obj['dbname']
    logger = logging.getLogger('config')
    handler = logging.StreamHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.info('load dbconfig: hostname=%s, port=%d, username=%s, passwd=%s, dbname=%s' % (DBCONFIG.HOSTNAME, DBCONFIG.PORT, DBCONFIG.USERNAME, DBCONFIG.PASSWD, DBCONFIG.DBNAME))
