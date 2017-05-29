import argparse
import json
import warnings

import MySQLdb
import datetime

from com.waylava.log.log import Logger
from com.waylava.log.log_style import BraceMessage
from com.waylava.mysql.cursor.cursor import Cursor
from com.waylava.mysql.db.dal import DAL
from com.waylava.mysql.execute.executor import Executor
from com.waylava.mysql.select.selector import Selector
from com.waylava.mysql.table.table import Table
from com.waylava.twitter.search import Search
from com.waylava.util.manager import Manager


class Scraper(object):
    def __init__(self):
        self.manager = Manager()
        self.mysql = self.get_connector()
        self.constraints = dict()
        self.keywords_file_path, self.constraints_file_path = self.get_args()
        self.keywords_file_path = self.keywords_file_path.strip()
        self.constraints_file_path = self.constraints_file_path.strip()
        self.load_constraints()
        self.dal = DAL()
        self.cursor = Cursor()
        self.executor = Executor()
        self.table = Table()
        self.selector = Selector()
        self.db_name = self.constraints['database']  # MySQL Database
        self.db_conn = None
        self.setup_mysql()
        self.chunk = list()
        self.processed = 0
        warnings.filterwarnings('ignore', category=MySQLdb.Warning)

    def get_connector(self):
        try:
            db_credentials = self.manager.get_db_credentials()
            host = db_credentials["mysql"]["host"]
            port = db_credentials["mysql"]["port"]
            user = db_credentials["mysql"]["user"]
            password = db_credentials["mysql"]["password"]
            connector = DAL(user, password, host, port)
            return connector
        except Exception as e:
            Logger.logger.error(e)

    def load_constraints(self):
        try:
            with open(self.constraints_file_path, "r+") as constraints_file:
                for line in constraints_file.readlines():
                    key, value = line.split(":")
                    key = key.strip()
                    value = value.strip()
                    self.constraints[key] = value
        except Exception as e:
            Logger.logger.error(e)

    @staticmethod
    def get_args():
        try:
            parser = argparse.ArgumentParser(description='Twitter Tweet Scraper')
            parser.add_argument('-k', '--keywords', type=str, help='Path to keywords file', required=True)
            parser.add_argument('-c', '--constraints', type=str, help='Path to constraints file', required=True)
            args = parser.parse_args()
            keywords_file_path = args.keywords
            constraints_file_path = args.constraints
            return keywords_file_path, constraints_file_path
        except Exception as e:
            Logger.logger.error(e)

    def setup_mysql(self):
        # Get a cursor depending on the nature of executable query.
        cursor = self.cursor.cursor(self.mysql.connect())
        Logger.logger.info("Creating crawler database ...")
        self.dal.create_db(cursor, self.db_name)
        try:
            self.db_conn = self.mysql.db_connect(self.db_name)
            self.db_conn.set_character_set('utf8')
        except MySQLdb.Error, e:
            Logger.logger.error(BraceMessage("Failed to establish connection to  database: {0}", e))
        db_cursor = self.cursor.cursor(self.db_conn)
        self.create_tweets_table(db_cursor)

    def create_tweets_table(self, db_cursor):
        table_name = '`tweets`'
        cols = ['`LANGUAGE`', '`PERMALINK`', '`USER_ID`', '`NAME`', '`TIMESTAMP`', '`TWEET_TEXT`',
                '`LOCATION`', '`URLS`', '`MENTIONS`', '`RETWEET_COUNT`', '`FAVORITE_COUNT`', '`SCREEN_NAME`']
        types = ['VARCHAR(8)', 'TEXT', 'VARCHAR(32)', 'TEXT', 'VARCHAR(32)', 'TEXT', 'TEXT', 'TEXT', 'TEXT',
                 'VARCHAR(8)', 'VARCHAR(8)', 'TEXT']
        schema = self.table.construct_schema(table_name, cols, types)
        self.table.create_table(schema, db_cursor)

    def run(self):
        search = Search()
        if len(self.constraints['miles']) > 0:
            near = " near:\"" + self.constraints['location'] + "\" within:" + self.constraints['miles'] + "mi"
        else:
            near = " near:\"" + self.constraints['location'] + "\""
        queries = list()
        since_until = ' since:' + self.constraints["from"] + " " + 'until:' + self.constraints["to"]
        with open(self.keywords_file_path, "r+") as keywords_file:
            for line in keywords_file:
                keyword = line.strip().lower()
                if near:
                    query = keyword + since_until + near
                else:
                    query = keyword + since_until
                queries.append(query)

        for query in queries:
            tweets = search.search(query)
            for tweet in tweets:
                dt = datetime.datetime.fromtimestamp(tweet['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
                if not tweet['location']:
                    location = tweet['location']
                else:
                    location = tweet['location'].encode('unicode-escape')
                self.chunk.append((tweet['lang'], tweet['permalink'], tweet['user_id'],
                                   tweet['name'].encode('unicode-escape'), dt,
                                   tweet['tweet_text'].encode('unicode-escape'), location,
                                   json.dumps(tweet['urls']), json.dumps(tweet['mentions']),
                                   tweet['retweet_count'], tweet['favorite_count'],
                                   tweet['screen_name'].encode('unicode-escape')))
                if len(self.chunk) % 10 == 0:
                    self.insert_tweets(self.chunk)
                    self.chunk = []
            if len(self.chunk) < 10:
                self.insert_tweets(self.chunk)
                self.chunk = []

    def insert_tweets(self, chunk):
        # print "Inserting records into `tweets` table"
        table_name = '`tweets`'
        cols = ['`LANGUAGE`', '`PERMALINK`', '`USER_ID`', '`NAME`', '`TIMESTAMP`', '`TWEET_TEXT`',
                '`LOCATION`', '`URLS`', '`MENTIONS`', '`RETWEET_COUNT`', '`FAVORITE_COUNT`', '`SCREEN_NAME`']
        query = """INSERT INTO %s (""" % table_name
        cols = ', '.join(cols)
        query += cols + ') '
        query += """VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        self.processed += len(chunk)
        Logger.logger.info(BraceMessage("Inserting chunk into `tweets` table: {0} {1}", self.processed, len(chunk)))
        self.executor.execute_write_many(self.db_conn, query, chunk)

if __name__ == "__main__":
    scraper = Scraper()
    scraper.run()