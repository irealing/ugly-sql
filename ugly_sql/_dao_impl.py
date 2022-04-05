# coding:utf-8
import logging

from ._db import Query, Table, GroupBy
from ._util import DBObjProxy

__author__ = 'Memory_Leak<irealing@163.com>'


class DBQuery:
    """
    数据库查询
    """
    logger = logging.getLogger(__name__)

    def __init__(self, session, table, *columns):
        """
        初始化
        :param session:数据库会话
        :param columns: 查询的字段列表
        """
        if not columns:
            columns = (table,)
        self.__columns = columns
        self.__sess = session
        self.__sql_query = Query(table, *columns)
        self.__mapping, self.__index = self.__obj_mapping(columns)

    @staticmethod
    def __obj_mapping(cols):
        """
        对象映射关系
        :param cols:
        :return:
        """
        cursor = 0
        i = 0
        mapping = []
        index = []
        while i < len(cols):
            c = cols[i]
            i += 1
            if isinstance(c, Table):
                index.append(cursor)
                mapping.append((c, cursor, cursor + c.col_size()))
                cursor += c.col_size()
            else:
                cursor += 1
        return mapping, index

    def filter(self, *fs):
        self.__sql_query.filter(*fs)
        return self

    def join(self, table, on):
        """
        INNER JOIN
        :param table:
        :param on:
        :return:
        """
        self.__sql_query.join(table, on)
        return self

    def left_join(self, table, on):
        """
        LEFT JOIN
        :param table:
        :param on:
        :return:
        """
        self.__sql_query.left_join(table, on)
        return self

    def right_join(self, table, on):
        """
        RIGHT JOIN
        :param table:
        :param on:
        :return:
        """
        self.__sql_query.right_join(table, on)
        return self

    def limit(self, limit, offset=0):
        self.__sql_query.limit(limit, offset)
        return self

    def all(self):
        return map(self.__render, self.__query())

    def one(self):
        row = self.__query(False)
        if not row:
            return None
        return self.__render(row)

    def scalar(self):
        r = self.one()
        if not r:
            return -1
        return self.one()[0]

    def __query(self, rows=True):
        sql = self.__sql_query.sql()
        args = self.__sql_query.args()
        return self.__sess.query(sql, args) if rows else self.__sess.one(sql, args)

    def __render(self, row):
        """
        数据库查询结果生成对象
            (如果有的话)
        :param row:
        :return:
        """
        if not self.__mapping:
            return row
        t, s, e = self.__mapping[0]
        if s == 0 and e == len(row):
            return DBObjProxy(t, dict(zip(t.table_columns_(), row)), self.__sess)
        rdata = []
        cursor = 0
        for i in range(len(self.__index)):
            idx = self.__index[i]
            if cursor < idx:
                rdata.extend(row[cursor:idx])
            t, s, e = self.__mapping[i]
            r = DBObjProxy(t, dict(zip(t.table_columns_(), row[s:e])), self.__sess)
            rdata.append(r)
            cursor = e
        if cursor < len(row):
            rdata.extend(row[cursor:])
        return rdata

    def order_by(self, *order):
        self.__sql_query.order_by(*order)
        return self

    def group_by(self, *col):
        self.__sql_query.group_by(GroupBy(*col))
        return self
