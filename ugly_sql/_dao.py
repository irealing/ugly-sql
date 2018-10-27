# coding:utf-8
import logging

from ._dao_impl import DBQuery
from ._session import DBSession

__author__ = 'Memory_Leak<irealing@163.com>'


class SessionManager(object):
    """
    数据对象操作会话
    """
    logger = logging.getLogger("SessionManager")

    def __init__(self, conn):
        self.session = DBSession(conn)

    def begin(self):
        self.session.begin()

    def __enter__(self):
        self.session.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.__exit__(exc_type, exc_val, exc_tb)

    def query(self, table, *cols):
        return DBQuery(self.session, table, *cols)

    def commit(self):
        return self.session.commit()

    def create(self, table, **kwargs):
        return self.session.create(table, **kwargs)
