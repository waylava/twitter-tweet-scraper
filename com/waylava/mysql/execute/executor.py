import MySQLdb
import time

import sys

from com.waylava.log.log import Logger
from com.waylava.log.log_style import BraceMessage
from com.waylava.mysql.db.dal import DAL


class Executor(object):
    def __init__(self):
        self.dal = DAL()

    @staticmethod
    def execute_get_dict(dict_cursor, query):
        attempts = 0
        while True:
            try:
                dict_cursor.execute(query)
                data = dict_cursor.fetchall()
                break
            except MySQLdb.Error, e:
                attempts += 1
                Logger.logger.error(BraceMessage("MySQL corpus db error on {0}\n{1}\n({2} attempt)", query, e, attempts))
                time.sleep(DAL.MYSQL_TIMEOUT)
                if attempts > DAL.MAX_ATTEMPTS:
                    sys.exit(1)
        return data

    @staticmethod
    def execute_write_many(db_conn, query, rows, write_cursor=None):
        if not write_cursor:
            write_cursor = db_conn.cursor()
        attempts = 0
        while True:
            try:
                write_cursor.executemany(query, rows)
                break
            except MySQLdb.Error, e:
                attempts += 1
                print(BraceMessage("MySQL corpus db error on {0}\n{1}\n({2} attempt)", query, e, attempts))
                time.sleep(DAL.MYSQL_TIMEOUT)
                write_cursor = db_conn.cursor()
                if attempts > DAL.MAX_ATTEMPTS:
                    sys.exit(1)
        return write_cursor
