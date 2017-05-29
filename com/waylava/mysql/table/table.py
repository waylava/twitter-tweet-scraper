import itertools

import MySQLdb

from com.waylava.log.log import Logger
from com.waylava.log.log_style import BraceMessage


class Table(object):
    def __init__(self):
        self.idx_cols = []

    def construct_schema(self, table_name, cols, types):
        try:
            query = """CREATE TABLE IF NOT EXISTS %s (`ID` MEDIUMINT(8) UNSIGNED NOT NULL AUTO_INCREMENT,""" % table_name
            Logger.logger.info(BraceMessage("Created schema for table {0}", table_name))
            for col, type_ in itertools.izip_longest(cols, types):
                query += col + ' ' + type_ + ','
            for col in cols:
                if col in self.idx_cols:
                    idx = col.replace('`', '') + '_IDX'
                    query += ' KEY ' + '`' + idx + '`' + ' (' + col + '),'
            query += 'PRIMARY KEY (`ID`));'
            return query
        except MySQLdb.Error, e:
            Logger.logger.error(e)

    @staticmethod
    def create_table(schema, cursor):
        try:
            cursor.execute(schema)
        except MySQLdb.Error, e:
            Logger.logger.error(e)

    @staticmethod
    def insert_records(cursor, cols, values, table_name=None):
        try:
            query = """INSERT INTO %s (""" % table_name
            cols = ', '.join(cols)
            query += cols + ') '
            query += """VALUES %s""" % str(values) + ';'
            cursor.execute(query)
        except MySQLdb.Error, e:
            Logger.logger.error(e)

    @staticmethod
    def read_records(cursor, table_name):
        try:
            query = """SELECT * FROM %s;""" % table_name
            cursor.execute(query)
            return cursor.fetchall()
        except MySQLdb.Error, e:
            Logger.logger.error(e)
