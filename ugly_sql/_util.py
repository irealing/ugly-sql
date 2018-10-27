# coding:utf-8
from ._db import Update

__author__ = 'Memory_Leak<irealing@163.com>'


class DBObjProxy(object):
    """
    对象代理工具
    """

    def __init__(self, table, obj, bind_session):
        self.__dict__['table'] = table
        self.__dict__['_raw'] = obj
        self.__dict__['_field_cache'] = {}
        self.__dict__['_binding'] = False
        self.__dict__['_update'] = {}
        self.__dict__['_bind_session'] = bind_session

    def __getattr__(self, item):
        if item in self._field_cache:
            return self._field_cache[item]
        if item not in self._raw:
            raise AttributeError("'{}' object has not attribute '{}'".format(self.__class__.__name__, item))
        v = self._raw[item]
        if isinstance(v, dict):
            data = self.__class__(v)
            self._field_cache[item] = data
        else:
            data = self._raw[item]
        return data

    def __dir__(self):
        return self._raw.keys()

    def __setattr__(self, key, value):
        if key not in self.table.table_columns_() or key == self.table.primary_key.name:
            raise AttributeError("attribute '{}' of '{}' object is read-only ".format(key, self.__class__.__name__))
        self._update[key] = getattr(self.table, key) == value
        self._raw[key] = value
        if not self._binding:
            self._bind_session._register_modify(self)

    def __modify__(self):
        m = Update(self.table)
        map(m.set, self._update.values())
        m.where(self.table.primary_key == self._raw[self.table.primary_key.name])
        return m

    def as_dict(self):
        """
        返回字典
        :return:
        """
        return self._raw
