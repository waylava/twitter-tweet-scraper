from com.waylava.log.log import Logger


class Scraper(object):
    def __init__(self):
        pass

    @staticmethod
    def scrape():
        Logger.logger.info("Testing log ...")
        pass

if __name__ == "__main__":
    scraper = Scraper()
    scraper.scrape()