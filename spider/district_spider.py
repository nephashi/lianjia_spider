from spider.base_spider import Spider
from bs4 import BeautifulSoup
import datetime

class DistrictSpider(Spider):

    def get_name(self):
        return 'district'

    def start_urls(self):
        return [
            "https://bj.lianjia.com/ershoufang/dongcheng",
            "https://bj.lianjia.com/ershoufang/xicheng",
            "https://bj.lianjia.com/ershoufang/chaoyang",
            "https://bj.lianjia.com/ershoufang/haidian",
            "https://bj.lianjia.com/ershoufang/fengtai",
            "https://bj.lianjia.com/ershoufang/shijingshan",
            "https://bj.lianjia.com/ershoufang/tongzhou",
            "https://bj.lianjia.com/ershoufang/changping",
            "https://bj.lianjia.com/ershoufang/daxing",
            "https://bj.lianjia.com/ershoufang/yizhuangkaifaqu",
            "https://bj.lianjia.com/ershoufang/shunyi",
            "https://bj.lianjia.com/ershoufang/fangshan",
            "https://bj.lianjia.com/ershoufang/mentougou",
            "https://bj.lianjia.com/ershoufang/pinggu",
            "https://bj.lianjia.com/ershoufang/huairou",
            "https://bj.lianjia.com/ershoufang/miyun",
            "https://bj.lianjia.com/ershoufang/yanqing"
        ]

    def parse(self, url, resp, date=None, **kwargs):
        if date is None:
            date = datetime.date.today()
        superior = url.split('/')[-1]
        content = resp.content.decode()
        soup = BeautifulSoup(content)
        divone = soup.select('div[data-role="ershoufang"]')[0]
        divtwo = list(divone.find_all("div"))[1].find_all("a")
        objs = []
        for item in divtwo:
            href = item.attrs['href']
            name = item.string
            if len(href) == 0 or len(name) == 0:
                continue
            if href[0] != '/':
                href = '/' + href
            objs.append({
                'url': 'https://bj.lianjia.com' + href,
                'name': name,
                'superior': superior,
                'date': date
            })
        return objs, [], {}

