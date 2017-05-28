import random

from com.waylava.model.proxy import Proxy
from com.waylava.util.manager import Manager


class Picker(object):
    def __init__(self):
        self.proxies = list()
        self.manager = Manager()
        self.setup()

    def setup(self):
        credentials = self.manager.get_proxy_credentials()
        username = credentials["proxy-servers"]["username"]
        password = credentials["proxy-servers"]["password"]
        id_ = 0
        for ip in self.manager.get_proxies():
            proxy = Proxy()
            proxy.ip = ip
            proxy.port = 6060
            proxy.username = username
            proxy.password = password
            id_ += 1
            self.proxies.append({'id': id_, 'url': {'http': self.manager.construct_url(proxy)}})

    def get_random_proxy(self):
        return random.choice(self.proxies)
