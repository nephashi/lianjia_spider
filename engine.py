from collections import deque
import requests
from config import CRAWLCONFIG
import logging
import json
import time
import datetime
from collections import defaultdict
from dao.dao_singleton import TASK_STATISTIC_DAO

RETRY_TIMES = CRAWLCONFIG.RETRY_TIMES
INTERVAL_TIME_SECOND = CRAWLCONFIG.INTERVAL_TIME_SECOND
SKIP_DUPLICATE_URL = CRAWLCONFIG.SKIP_DUPLICATE_URL

crawled_url = set()
headers= {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"
}

def run_spider(spider, dao, date=None):
    logger = logging.getLogger('engine_' + spider.get_name())
    handler = logging.StreamHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.info('spider %s start' % spider.get_name())
    start_time = time.time()
    if date is None:
        date = datetime.date.today()
    date_str = date.strftime('%y-%m-%d')
    url_queue = deque(maxlen=10000)
    statistic = defaultdict(int)
    start_urls = spider.start_urls()
    if start_urls is not None:
        for url in start_urls:
            url_queue.append(url)
    while len(url_queue) > 0:
        url = url_queue.popleft()
        if SKIP_DUPLICATE_URL and url in crawled_url:
            logger.info('%s has been crawled, skip', url)
            continue
        retry_cnt = 0
        succ = False
        while retry_cnt < RETRY_TIMES:
            if INTERVAL_TIME_SECOND > 0:
                time.sleep(INTERVAL_TIME_SECOND)
            resp = requests.get(url, headers=headers)
            status_code = resp.status_code if resp else 0
            statistic['status_code_' + str(status_code)] += 1
            logger.info('request %s for %dth time, status_code=%d' % (url, retry_cnt, status_code))
            if resp and resp.status_code == 200:
                try:
                    objs, new_urls, custom_statistic = spider.parse(url, resp, date)
                except:
                    objs = None
                    new_urls = None
                    custom_statistic = None
                if objs is not None:
                    logger.info('parse succ. url=%s, item num=%d, new url num=%d, retry=%d' % (url, len(objs), len(new_urls), retry_cnt))
                    if len(objs) > 0:
                        dao.insert_many(objs)
                    for new_url in new_urls:
                        url_queue.appendleft(new_url)
                        logger.info('new url %s from %s' % (new_url, url))
                    statistic['collected_item'] += len(objs)
                    succ = True
                    crawled_url.add(url.rstrip('\r\n/'))
                    for custom_statistic_k, custom_statistic_v in custom_statistic.items():
                        if type(custom_statistic_v) != int:
                            continue
                        custom_statistic_k = 'custom_' + custom_statistic_k
                        statistic[custom_statistic_k] += custom_statistic_v
                    break
                else:
                    logger.error('parse error. url=%s, retry=%d' % (url, retry_cnt))
                    retry_cnt += 1
            else:
                retry_cnt += 1
        if succ:
            statistic['succ_url'] += 1
        else:
            statistic['fail_url'] += 1
    statistic = {k: v for k, v in statistic.items()}
    cost_time = time.time() - start_time
    statistic['time_cost'] = cost_time
    logger.info('spider %s done, time cost %f' % (spider.get_name(), cost_time))
    logger.info('collected item num: ' + str(statistic.get('collected_item', 0)))
    logger.info('succ url num: ' + str(statistic.get('succ_url', 0)))
    logger.info('fail url num: ' + str(statistic.get('fail_url', 0)))
    status_l = []
    for k, v in statistic.items():
        if k.startswith('status_code'):
            status_l.append((k, v))
    status_l.sort(key=lambda x: -x[1])
    for item in status_l:
        logger.info('%s num: %d' % (item[0], item[1]))
    statistic_json = json.dumps(statistic)
    TASK_STATISTIC_DAO.insert({
        'spider_name': spider.get_name(),
        'date': date_str,
        'time_cost': statistic['time_cost'],
        'num_collected_item': statistic.get('collected_item', 0),
        'num_succ_url': statistic.get('succ_url', 0),
        'num_fail_url': statistic.get('fail_url', 0),
        'raw_json': statistic_json,
    })

