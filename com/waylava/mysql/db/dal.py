import sys
import time
import warnings

import MySQLdb

from com.waylava.log.log import Logger
from com.waylava.log.log_style import BraceMessage


class DAL(object):
    MAX_ATTEMPTS = 5  # max number of times to try a query before exiting
    MYSQL_TIMEOUT = 4  # number of seconds to wait before trying a query again in case of a failure

    def __init__(self, user=None, password=None, host=None, port=None):
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    @staticmethod
    def create_db(conn, db):
        try:
            with warnings.catch_warnings(record=True) as _:
                # Cause all warnings to always be triggered.
                warnings.simplefilter("always")
                query = 'CREATE DATABASE IF NOT EXISTS %s' % db
                conn.execute(query)
                Logger.logger.info(BraceMessage("Database {0} already exists!", db))

        except MySQLdb.Error, e:
            Logger.logger.error(e)
        Logger.logger.info("Closing MySQL connection ...")
        conn.close()

    def connect(self):
        attempts = 0
        while True:
            try:
                conn = MySQLdb.connect(host=self.host,
                                       port=self.port,
                                       user=self.user,
                                       passwd=self.password)
                conn.autocommit(True)
                return conn
            except MySQLdb.Error, e:
                attempts += 1
                Logger.logger.error(BraceMessage("MySQL connection error: {0}\n({1} attempt)", e, attempts))
                time.sleep(DAL.MYSQL_TIMEOUT)
                if attempts > DAL.MAX_ATTEMPTS:
                    sys.exit(1)

    def db_connect(self, db=None):
        attempts = 0
        while True:
            try:
                db_conn = MySQLdb.connect(host=self.host,
                                          port=self.port,
                                          user=self.user,
                                          passwd=self.password,
                                          db=db)
                db_conn.autocommit(True)
                break
            except MySQLdb.Error, e:
                attempts += 1
                Logger.logger.error(BraceMessage("MySQL connection error on DB: {0}\n{1}\n({2} attempt)", db, e, attempts))
                time.sleep(DAL.MYSQL_TIMEOUT)
                if attempts > DAL.MAX_ATTEMPTS:
                    sys.exit(1)
        return db_conn
