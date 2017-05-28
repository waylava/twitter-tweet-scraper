import urllib2
from random import choice

from BeautifulSoup import BeautifulSoup as Bs

from com.waylava.log.log import Logger
from com.waylava.util.paths import Paths


class UserAgents:
    def __init__(self):
        self.agents = []
        self.load_agents()

    @staticmethod
    def get_agents_data():
        url = "http://www.useragentstring.com/pages/useragentstring.php?name=All"
        resources_path = Paths.get_resources_path()
        try:
            data = urllib2.urlopen(url)
            file_ = open(resources_path + "/randuseragentstring.html", 'w')
            aux = data.read()
            file_.write(aux)
            file_.close()
            data.close()
            return aux
        except urllib2.URLError, e:
            Logger.logger.error(e)
            aux_file = resources_path + "/randuseragentstring.html"
            return open(aux_file, 'r')

    def load_agents(self):
        try:
            data = self.get_agents_data()
            soup = Bs(data)
            links = soup.findAll('a')
            for link in links:
                try:
                    if link['href']:
                        user_agent = link.string
                        if user_agent and user_agent.startswith("Mozilla") and 'Windows' in user_agent and \
                                        'AppleWebKit' in user_agent and 'Chrome' in user_agent:
                            self.agents.append(user_agent)
                except Exception as _:
                    pass
        except Exception, e:
            Logger.logger.error(e)

    def get_one(self, rand=True):
        if rand:
            if len(self.agents) == 0:
                return ""
            else:
                return choice(self.agents)

    def get_agents(self):
        return self.agents
