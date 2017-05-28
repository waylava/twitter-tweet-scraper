from com.waylava.proxy.picker import Picker


class Counter(object):
    def __init__(self):
        self.rate_limits_transient = dict()
        self.total_requests = dict()
        self.setup()

    def setup(self):
        picker = Picker()
        for proxy in picker.proxies:
            self.rate_limits_transient[proxy['id']] = {'req': 0, 'start_time': None}
            self.total_requests[proxy['id']] = 0

    def increment_requests(self, id_):
        self.rate_limits_transient[id_]['req'] += 1

    def reset_requests(self, id_):
        self.rate_limits_transient[id_]['req'] = 0

    def get_requests(self, id_):
        return self.rate_limits_transient[id_]['req']

    def set_start_time(self, id_, time):
        self.rate_limits_transient[id_]['start_time'] = time

    def get_start_time(self, id_):
        return self.rate_limits_transient[id_]['start_time']
