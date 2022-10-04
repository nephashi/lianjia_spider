from spider.district_spider import DistrictSpider
from spider.sellinfo_spider import SellInfoSpider
from dao.dao_singleton import DISTRICT_DAO, SELL_INFO_DAO
from engine import run_spider
import datetime

date = datetime.date.today()

district_spider = DistrictSpider()
sell_info_spider = SellInfoSpider(DISTRICT_DAO, date)

spiders = [district_spider, sell_info_spider]
daos = [DISTRICT_DAO, SELL_INFO_DAO]

assert len(daos) == len(spiders)

for i in range(len(spiders)):
    spider = spiders[i]
    dao = daos[i]
    run_spider(spider, dao, date)