import MySQLdb

from com.waylava.log.log import Logger


class Cursor(object):
    @staticmethod
    def cursor(conn):
        try:
            cursor = conn.cursor()
            return cursor
        except MySQLdb.Error, e:
            Logger.logger.error(e)

    @staticmethod
    def dict_cursor(conn):
        try:
            dict_cursor = conn.cursor(MySQLdb.cursors.DictCursor)
            return dict_cursor
        except MySQLdb.Error, e:
            Logger.logger.error(e)

    @staticmethod
    def ss_cursor(conn):
        try:
            ss_cursor = conn.cursor(MySQLdb.cursors.SSCursor)
            return ss_cursor
        except MySQLdb.Error, e:
            Logger.logger.error(e)
