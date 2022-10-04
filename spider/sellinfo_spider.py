import logging

from spider.base_spider import Spider
from bs4 import BeautifulSoup
from collections import defaultdict

class SellInfoSpider(Spider):
    def __init__(self, district_dao, date):
        self.logger = logging.getLogger(self.get_name())
        handler = logging.StreamHandler()
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
        super().__init__()
        self.district_dao = district_dao
        self.date = date


    # def __init__(self):
    #     self.logger = logging.getLogger(spider.get_name())
    #     handler = logging.StreamHandler()
    #     self.logger.addHandler(handler)
    #     self.logger.setLevel(logging.INFO)
    #     pass

    def get_name(self):
        return "sell_info"

    def start_urls(self):
        self.district_info = self.district_dao.select_with_date(self.date)
        self.cur_district_idx = 0
        if len(self.district_info) > 0:
            return [self.district_info[0]['url']]
        return []

    def parse_price(self, price_str):
        tmp = ''
        for ch in price_str:
            if ch.isdigit() or ch == '.':
                tmp += ch
        try:
            rt = float(tmp)
        except:
            rt = 0.0
        return rt

    def parse_area(self, area_str):
        tmp = ''
        for ch in area_str:
            if ch.isdigit() or ch == '.':
                tmp += ch
        try:
            rt = float(tmp)
        except:
            rt = 0.0
        return rt

    def parse_one_item(self, li, date, district, superior):
        lianjia_id = ""
        link = ""
        img_link = ""
        title = ""
        goodhouse = False
        position = ""
        detail = ""
        follow = ""
        tags = []
        area = 0
        total_price_str = ""
        total_price = 0
        unit_price_str = ""
        unit_price = 0

        img_tag = li.select('a.img')
        if len(img_tag) > 0:
            img_tag = img_tag[0]
            link = img_tag.attrs['href']
            inner_img_tag = img_tag.select('img.lj-lazy')
            if len(inner_img_tag) > 0:
                inner_img_tag = inner_img_tag[0]
                img_link = inner_img_tag.attrs['src']

        title_tag = li.select('div.title')
        if len(title_tag) > 0:
            title_tag = title_tag[0]
            inner_title_tag = title_tag.select('a')
            if len(inner_title_tag) > 0:
                inner_title_tag = inner_title_tag[0]
                title = inner_title_tag.text
                link_ = inner_title_tag.attrs['href']
                if link == "":
                    link = link_
            goodhouse_tag = title_tag.select('span.goodhouse_tag')
            if len(goodhouse_tag) > 0:
                goodhouse = True
        if len(link) > 0:
            link_last_part = link.split('/')[-1]
            if link_last_part.endswith('.html'):
                lianjia_id = link_last_part[:-5]
            elif link_last_part.endswith('.htm'):
                lianjia_id = link_last_part[:-4]
        position_tag = li.select('div.positionInfo')
        if len(position_tag) > 0:
            position = position_tag[0].text.replace(' ', '')
        detail_tag = li.select('div.houseInfo')
        if len(detail_tag) > 0:
            detail = detail_tag[0].text
        detail_splits = detail.split('|')
        if len(detail_splits) >= 2:
            area = self.parse_area(detail_splits[1])
        follow_tag = li.select('div.followInfo')
        if len(follow_tag) > 0:
            follow = follow_tag[0].text
        tag_tag = li.select('div.tag')
        if len(tag_tag) > 0:
            tag_tag = tag_tag[0]
            for tag_child in tag_tag.children:
                tags.append(tag_child.text)
        total_price_tag = li.select('div.totalPrice')
        if len(total_price_tag) > 0:
            total_price_tag = total_price_tag[0]
            total_price_str = total_price_tag.text
            total_price = self.parse_price(total_price_str)
        unit_price_tag = li.select('div.unitPrice')
        if len(unit_price_tag) > 0:
            unit_price_tag = unit_price_tag[0]
            unit_price_str = unit_price_tag.text
            unit_price = self.parse_price(unit_price_str)
        return {
            'lianjia_id': lianjia_id,
            'link': link,
            'title': title,
            'goodhouse': goodhouse,
            'position': position,
            'detail': detail,
            'follow': follow,
            'tags': tags,
            'area': area,
            'total_price_str': total_price_str,
            'total_price': total_price,
            'unit_price_str': unit_price_str,
            'unit_price': unit_price,
            'district': district,
            'superior': superior,
            'date': date
        }


    def parse(self, url, resp, date=None, **kwargs):
        district = self.district_info[self.cur_district_idx]['name']
        superior = self.district_info[self.cur_district_idx]['superior']
        date = self.date
        statistic = defaultdict(int)

        page = 1
        url_splits = url.rstrip('/\r\n').split('/')
        url_last_part = url_splits[-1]
        if url_last_part.startswith('pg'):
            try:
                page = int(url_last_part[2:])
            except:
                page = 0

        parse_items = []
        content = resp.content.decode()
        soup = BeautifulSoup(content)
        content_list = soup.select('ul.sellListContent')
        # 没有项目，说明这个地区到头了
        # 要是到100了，可能是哪里有问题死循环了，也跳转到下一个地区。
        if len(content_list) == 0 or page == 100:
            if self.cur_district_idx == len(self.district_info) - 1:
                return parse_items, [], {}
            else:
                self.cur_district_idx += 1
                next_district_url = self.district_info[self.cur_district_idx]['url']
                return parse_items, [next_district_url], {}
        content_list = content_list[0]
        lis = content_list.find_all("li")
        for li in lis:
            try:
                item = self.parse_one_item(li, date, district, superior)
            except:
                item = None
            if item is not None:
                parse_items.append(item)
            else:
                statistic['sell_info_parse_fail'] += 1
        new_urls = []

        # page解析成功，跳转到下一页
        if page > 0:
            page += 1
            if url_last_part.startswith('pg'):
                url_splits[-1] = 'pg' + str(page)
            else:
                url_splits.append('pg' + str(page))
            new_urls.append('/'.join(url_splits))
            return parse_items, new_urls, statistic
        # page == 0: 解析错了，跳到下一个地区
        else:
            if self.cur_district_idx == len(self.district_info) - 1:
                return parse_items, [], {}
            else:
                self.cur_district_idx += 1
                next_district_url = self.district_info[self.cur_district_idx]['url']
                return parse_items, [next_district_url], {}

#
# spider = SellInfoSpider()
# url = 'https://bj.lianjia.com/ershoufang/andingmen/pg8'
# resp = requests.get(url)
# spider.parse(url, resp)




