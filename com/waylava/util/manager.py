from com.waylava.log.log import Logger
from com.waylava.util.paths import Paths
import yaml


class Manager(object):
    def __init__(self):
        self.resources_path = Paths.get_resources_path()

    def get_db_credentials(self):
        pass

    def get_proxies(self):
        try:
            with open(self.resources_path + "/proxies", 'r') as f:
                for proxy in f.readlines():
                    proxy = proxy.strip()
                    yield proxy
        except Exception as e:
            Logger.logger.error(e)

    def get_proxy_credentials(self):
        try:
            with open(self.resources_path + "/proxy-credentials.yml", 'r') as yml:
                return yaml.load(yml)
        except Exception as e:
            Logger.logger.error(e)

    @staticmethod
    def construct_url(proxy):
        auth_string = "http://%s:%s@%s:%s" % (proxy.username,
                                              proxy.password,
                                              proxy.ip,
                                              proxy.port)
        return auth_string

if __name__ == "__main__":
    manager = Manager()
    print manager.get_proxy_credentials()

