import logging.config
import logging.handlers

from com.waylava.util.paths import Paths


def singleton(cls):
    instances = {}

    def get_instance():
        if cls not in instances:
            instances[cls] = cls()
        return instances[cls]

    return get_instance()


@singleton
class Logger:
    def __init__(self):
        resources_path = Paths().get_resources_path()
        config_file_path = resources_path + "/logging.conf"
        logging.config.fileConfig(config_file_path)
        self.logger = logging.getLogger('twitter-tweet-scraper.log')
        # create console handler with a higher log level
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        # set a format which is simpler for console use
        # See: https://docs.python.org/2/library/logging.html#logging.Formatter for more info
        formatter = logging.Formatter('%(name)s: %(levelname)s: %(asctime)2s: %(funcName)s: %(message)s',
                                      '%Y-%m-%d %H:%M:%S')
        # tell the handler to use this format
        console.setFormatter(formatter)
        # add the handler to the root logger
        self.logger.addHandler(console)
