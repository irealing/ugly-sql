# coding:utf-8
__author__ = 'Memory_Leak<irealing@163.com>'

import logging
import sys

from ._db import Insert
from ._util import DBObjProxy


class DBSession(object):
    """数据库会话工具"""
    logger = logging.getLogger("DBSession")

    def __init__(self, conn, transaction=False):
        self.__conn = conn
        self.__cursor = self.__conn.cursor()
        self.__begin = False
        self.__commit = False
        self.__tran = transaction
        self.__modify = []

    def _register_modify(self, update):
        self.__modify.append(update)

    def __enter__(self):
        if self.__tran:
            self.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.__begin and not self.__commit:
            self.__cursor.execute("ROLLBACK;")
        if exc_val:
            _, _, tb = sys.exc_info()
            if tb.tb_next is not None:
                tb = tb.tb_next
            cn = exc_val.__class__.__name__
            es = "{}:{}:{}".format(tb.tb_frame.f_code.co_filename, cn, tb.tb_lineno)
            logging.warning("db context error %s : %s", es, exc_val)

    def begin(self):
        if not self.__begin:
            self.__cursor.execute('BEGIN;')
            self.__begin = True

    def __query(self, sql, params, rows=False):
        """
        查询
        :param sql:
        :param rows:多行返回
        :param params:
        :return:
        """
        self.logger.debug("execute sql : %s", sql)
        self.__cursor.execute(sql, params)
        return self.__cursor.fetchall() if rows else self.__cursor.fetchone()

    def query(self, sql, params):
        """
        查询多行
        :param sql:
        :param params:
        :return:
        """
        return self.__query(sql, params, True)

    def one(self, sql, params):
        """
        查询一行
        :param sql:
        :param params:
        :return:
        """
        return self.__query(sql, params, False)

    def __do_modify(self):
        if not self.__modify:
            return
        while self.__modify:
            m = self.__modify.pop(0)
            mu = m.__modify__()
            self.update(mu.sql(), mu.args())

    def update(self, sql, params):
        """
        更新数据
        :param sql:
        :param params:
        :return:
        """
        self.__cursor.execute(sql, params)

    def commit(self, again=True):
        """
        提交改动
        :param again:
        :return:
        """
        if not self.__begin:
            raise Exception("must call begin before commit")
        self.__do_modify()
        self.__cursor.execute('COMMIT;')
        self.__commit = True
        if again:
            self.__begin = False
            self.begin()
            self.__commit = False

    def create(self, table, **kwargs):
        """
        生成记录
        :param table:
        :param kwargs:
        :return:
        """
        tk = set(table.table_columns_())
        ak = set(kwargs.keys())
        unexpected = ak - tk
        if unexpected:
            raise NameError(*unexpected)
        if table.primary_auto and table.primary_key.name in ak:
            raise AttributeError("auto-generate primary key %s".format(not self.primary_key.name))
        ins = Insert(table)
        for k, v in kwargs.items():
            ins.set(getattr(ins.table, k) == v)
        return self.__create(ins, table.primary_auto)

    def __create(self, ins, auto_id=False):
        """
        插入数据
        :param ins:
        :type ins:Insert
        :param auto_id:
        :return:
        """
        # cols = ins.table.table_columns_()
        args = list(ins.args())
        sql = ins.sql()
        self.__cursor.execute(sql, args)
        obj = dict(zip(ins.keys(), ins.args()))
        if auto_id:
            obj[ins.table.primary_key.name] = self.__cursor.lastrowid
        return DBObjProxy(ins.table, obj, self)
