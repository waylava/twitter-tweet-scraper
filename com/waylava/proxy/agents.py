import sys
import urllib2
from random import choice

from com.waylava.log.log import Logger
from com.waylava.util.paths import Paths

try:
    from BeautifulSoup import BeautifulSoup as bs
except ImportError:
    print ("BeautifulSoup can't be imported.")
    sys.exit()


class UserAgents:
    def __init__(self, length=25):
        """
        @param length: User Agent list length
        @type length: int
        """
        self.resources_path = Paths.get_resources_path()
        self._last = 0
        self._length = length
        self._list = []
        self.load_list()

    def get_data(self):
        """
        Get data from URL or File

        @return: URL or file data
        """
        url = "http://www.useragentstring.com/pages/useragentstring.php?name=All"
        try:
            data = urllib2.urlopen(url)
            file_ = open(self.resources_path + "/randuseragentstring.html", 'w')
            aux = data.read()
            file_.write(aux)
            file_.close()
            data.close()
            return aux
        except urllib2.URLError, e:
            Logger.logger.error(e)
            aux_file = self.resources_path + "/randuseragentstring.html"
            return open(aux_file, 'r')

    def load_list(self):
        """
        Generate variable length list of User Agent Strings

        @return: None
        """
        try:
            data = self.get_data()
            soup = bs(data)
            links = soup.findAll('a')
            for link in links:
                try:
                    if link['href']:
                        user_agent = link.string
                        if user_agent and user_agent.startswith("Mozilla") and 'Windows' in user_agent and \
                                        'AppleWebKit' in user_agent and 'Chrome' in user_agent:
                            self._list.append(link.string)
                except Exception as e:
                    # Handle 'href' errors
                    Logger.logger.error(e)
        except Exception, e:
            Logger.logger.error(e)

    def reload_list(self, length=-1):
        """
         Regenerate variable length list of User Agent Strings

         @return: None
         :param length:
        """
        if len != -1:
            self._length = length
        self._list = []
        self._last = 0
        self.load_list()

    def get_one(self, rand=True):
        """
        Get one User String from list using random or round-robin politic.

        @param rand: Determine used politic for selection
        @type rand: bool

        @return: User Agent string
        @rtype: string
        """
        if rand:
            if len(self._list) == 0:
                return ""
            else:
                return choice(self._list)
        else:
            self._last += 1
            if self._last == len(self._list):
                self._last = 0
            return self._list[self._last]

    def get_list(self):
        """
        Get all User Agent string list

        @return: User Agent string list
        @rtype: list
        """
        return self._list