import json
import urllib

import datetime
import requests

from com.waylava.log.log import Logger
from com.waylava.metrics.counter import Counter
from com.waylava.proxy.agents import UserAgents
from com.waylava.proxy.picker import Picker
from com.waylava.twitter.parse import Parser
from com.waylava.util.manager import Manager


class Search(object):
    def __init__(self):
        self.picker = Picker()
        self.agents = UserAgents()
        self.counter = Counter()
        self.manager = Manager()
        self.parser = Parser()

    @staticmethod
    def get_current_time():
        return datetime.datetime.now()

    @staticmethod
    def get_elapsed_time(a, b):
        elapsed_time = b - a
        # divmod returns quotient -> minutes and remainder -> seconds
        return divmod(elapsed_time.total_seconds(), 60)[0]

    @staticmethod
    def find_value(html, key):
        pos_begin = html.find(key) + len(key) + 2
        pos_end = html.find('"', pos_begin)
        return html[pos_begin: pos_end]

    @staticmethod
    def is_within_15_min_window(start_time, current_time):
        return Search.get_elapsed_time(start_time, current_time) < 12

    def is_within_rate_limit(self, id_):
        return self.counter.get_requests(id_) < 100

    def make_a_pick(self):
        while True:
            current_time = self.get_current_time()
            pick = self.picker.get_random_proxy()
            id_ = pick['id']
            start_time = self.counter.get_start_time(id_)
            if not start_time:
                valid_pick = pick
                self.counter.set_start_time(id_, self.get_current_time())
                break
            else:
                if self.is_within_15_min_window(start_time, current_time):
                    if self.is_within_rate_limit(id_):
                        valid_pick = pick
                        break
                    else:
                        continue
                else:
                    self.counter.set_start_time(id_, self.get_current_time())
                    self.counter.reset_requests(id_)
                    valid_pick = pick
                    break
        return valid_pick

    def search(self, term):
        twitter_urls = self.manager.get_twitter_urls()
        search_url = twitter_urls["end-points"]["search-url"]
        search_more_url = twitter_urls["end-points"]["search-more-url"]
        pick = self.picker.get_random_proxy()
        proxy = pick['url']
        id_ = pick['id']
        response = None
        try:
            user_agent = self.agents.get_one()
            response = requests.get(search_url.format(term=urllib.quote_plus(term)),
                                    headers={'User-agent': user_agent},
                                    proxies=proxy).text
        except Exception as e:
            Logger.logger.error(e)
        self.counter.increment_requests(id_)
        self.counter.set_start_time(id_, self.get_current_time())
        self.counter.total_requests[id_] += 1
        min_position = self.find_value(response, 'data-min-position')
        for tweet in self.parser.parse_search_results(response.encode('utf8')):
            yield tweet

        has_more_items = True
        while has_more_items:
            valid_pick = self.make_a_pick()
            valid_proxy = valid_pick['url']
            id_ = valid_pick['id']
            try:
                user_agent = self.agents.get_one()
                response = requests.get(search_more_url.format(term=urllib.quote_plus(term), max_position=min_position),
                                        headers={'User-agent': user_agent}, proxies=valid_proxy).text
            except Exception as e:
                Logger.logger.error(e)
            self.counter.increment_requests(id_)
            self.counter.total_requests[id_] += 1

            try:
                response_dict = json.loads(response)
                min_position = response_dict['min_position']
                has_more_items = response_dict['has_more_items']
                try:
                    for tweet in self.parser.parse_search_results(response_dict['items_html'].encode('utf8')):
                        yield tweet
                    has_more_items = True
                except Exception as _:
                    pass
            except Exception as _:
                pass
        Logger.logger.info(self.counter.total_requests)
