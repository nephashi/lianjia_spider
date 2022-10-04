from exception.not_implement_exception import NotImplementException

class Spider:
    def __init__(self):
        pass

    def get_name(self):
        return 'base'

    def start_urls(self):
        raise NotImplementException

    # return: objs, new_urls, custom_statistic
    # objs: list
    # new_urls: list
    # custom_statistic: dict
    def parse(self, url, resp, date=None, **kwargs):
        raise NotImplementException

    def clear(self):
        pass
