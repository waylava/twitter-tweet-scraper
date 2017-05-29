import MySQLdb

from com.waylava.log.log import Logger
from com.waylava.mysql.execute.executor import Executor


class Selector(object):
    def __init__(self):
        self.execute = Executor()

    def select_col(self, table_name, cursor, col, distinct=False):
        try:
            if distinct:
                query = """SELECT DISTINCT %s FROM %s;""" % (col, table_name)
            else:
                query = """SELECT %s FROM %s""" % (col, table_name)
            result_set = self.execute.execute_get_dict(cursor, query)
            return result_set
        except MySQLdb.Error, e:
            Logger.logger.error(e)
