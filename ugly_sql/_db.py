# coding:utf-8
from collections import OrderedDict

from ._patch import local_map

__author__ = 'Memory_Leak<irealing@163.com>'
map = local_map


class Table(object):
    """
    数据库表对象
    """

    def __init__(self, name, column_names, primary_key, primary_auto=True):
        self.table_name_ = name
        self.fields = OrderedDict((cn, Field(name, cn)) for cn in column_names)
        self.primary_key = self.fields.get(primary_key)
        self.primary_auto = primary_auto

    def __getattr__(self, item):
        if item in self.fields:
            return self.fields[item]
        raise AttributeError("'{}' object has no attribute '{}'".format(self.__class__.__name__, item))

    def table_columns_(self):
        """
        返回数据库表字段列表
        :return:list
        """
        return self.fields.keys()

    def col_size(self):
        return len(self.fields)


class SQLFragment(object):
    def __str__(self):
        return self.sql()

    def sql(self):
        raise NotImplemented


class OrderBy(SQLFragment):
    """
    排序ORDER BY
    """

    def __init__(self, *column):
        self.column = column
        self.order = "ASC"

    def sql(self):
        return "ORDER BY {} {}".format(",".join(map(lambda c: c.sql(), self.column)), self.order)

    def asc(self):
        self.order = "ASC"
        return self

    def desc(self):
        self.order = "DESC"
        return self


class GroupBy(SQLFragment):
    """
    GROUP BY语句
    """

    def __init__(self, *column):
        self.column = column

    def sql(self):
        return "GROUP BY {}".format(",".join(map(lambda c: c.sql(), self.column)))


class Field(SQLFragment):
    """
    字段
    """

    def __init__(self, table, name):
        self.table = table
        self.name = name

    def sql(self):
        return "`{}`.`{}`".format(self.table, self.name)

    def __lt__(self, other):
        return SimpleFilter(self, "<", other)

    def __le__(self, other):
        return SimpleFilter(self, "<=", other)

    def __gt__(self, other):
        return SimpleFilter(self, ">", other)

    def __ge__(self, other):
        return SimpleFilter(self, ">=", other)

    def __eq__(self, other):
        return SimpleFilter(self, "=", other)

    def __ne__(self, other):
        return SimpleFilter(self, "!=", other)

    def __str__(self):
        return self.sql()

    def in_(self, values):
        return InFilter(self, values)

    def is_(self, value):
        return SimpleFilter(self, "IS", value)

    def desc(self):
        return OrderBy(self).desc()

    def asc(self):
        return OrderBy(self).asc()

    def distinct(self):
        return WrapField("DISTINCT", self)

    def between(self, start, end):
        return _Between(self, start, end)


class FuncField(Field):
    """
    基础SQL函数
    """

    def __init__(self, func, base_filed):
        Field.__init__(self, base_filed.table, base_filed.name)
        self.__opc = base_filed
        self.__func = func.upper()

    def sql(self):
        return "{}({})".format(self.__func, self.__opc.sql())


class WrapField(Field):
    """
    包装字段
    """

    def __init__(self, wrap, base_field):
        Field.__init__(self, base_field.table, base_field.name)
        self.__wrap = wrap.upper()

    def sql(self):
        return "{} {}".format(self.__wrap, super(WrapField, self).sql())


class _Between(SQLFragment):
    def __init__(self, field, start, end):
        self._field = field
        self._start = start
        self._end = end

    def sql(self):
        return "{} BETWEEN %s AND %s".format(self._field.sql())

    def args(self):
        return self._start, self._end


class Filter(SQLFragment):
    """
    过滤条件
    """

    def sql(self):
        raise NotImplementedError("'sql' method not implemented")

    def args(self):
        raise NotImplementedError("'args' method not implemented")


class ORFilter(Filter):
    def __init__(self, *fs):
        self.__fs = fs

    def sql(self):
        return " OR ".join(map(lambda f: f.sql(), self.__fs))

    def args(self):
        params = []
        for f in self.__fs:
            params.extend(f.args())
        return params


class SimpleFilter(Filter):
    def __init__(self, column, operator, value):
        Filter.__init__(self)
        self.column = column
        self.operator = operator
        self.value = value
        self.with_column = isinstance(value, Field)

    def sql(self):
        return "{} {} {}".format(self.column.sql(), self.operator, "%s" if not self.with_column else self.value.sql())

    def args(self):
        return () if self.with_column else (self.value,)

    def __or__(self, other):
        return ORFilter(self, other)


class InFilter(SimpleFilter):
    """
    数据库IN操作
    """

    def __init__(self, column, values):
        super(InFilter, self).__init__(column, "IN", values)

    def sql(self):
        params = ",".join(map(lambda _: "%s", self.value))
        return "{} {} ({})".format(self.column.sql(), self.operator, params)

    def args(self):
        return self.value


class _Join(SQLFragment):
    """
    连接查询
    """
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"

    def __init__(self, table, method, on):
        self.table = table
        self.method = method
        self.on = on

    def sql(self):
        return "{} JOIN `{}` ON {}".format(self.method, self.table.table_name_, self.on.sql())


class Function(object):
    """
    SQL 基础函数
    """
    __instance = None

    @classmethod
    def instance(cls):
        if not cls.__instance:
            cls.__instance = cls()
        return cls.__instance

    def __getattr__(self, item):
        return lambda f: FuncField(item, f)


class Query(SQLFragment):
    """
    查询
    """

    def __init__(self, table, *fields):
        self.from_ = table
        self.filters = []
        if not fields:
            fields = (table,)
        self.fields = fields
        self.__join_filters = []
        self.__limit = None
        self.__args = []
        self.__order = None
        self.__group_by = None

    def filter(self, *fs):
        self.filters.extend(fs)
        map(lambda f: self.__args.extend(f.args()), fs)
        return self

    def args(self):
        if self.__limit:
            self.__args.extend(self.__limit)
        return tuple(self.__args)

    def __query_columns(self):
        return "*" if not self.fields else ",".join(
            map(lambda c: ",".join(map(lambda tc: tc.sql(), c.fields.values())) if isinstance(c, Table) else c.sql(),
                self.fields))

    def __join(self, method, table, on):
        self.__join_filters.append(_Join(table, method, on))
        return self

    def join(self, table, on):
        return self.__join(_Join.INNER, table, on)

    def left_join(self, table, on):
        return self.__join(_Join.LEFT, table, on)

    def right_join(self, table, on):
        return self.__join(_Join.RIGHT, table, on)

    def limit(self, limit, offset=0):
        self.__limit = (offset, limit)
        return self

    def sql(self):
        cs = self.__query_columns()
        sql = "SELECT {} FROM `{}`".format(cs, self.from_.table_name_)
        if self.__join_filters:
            sql = "{} {}".format(sql, " ".join(map(lambda j: j.sql(), self.__join_filters)))
        if self.filters:
            sql = "{} WHERE {}".format(sql, " AND ".join(map(lambda f: f.sql(), self.filters)))
        if self.__group_by:
            sql = "{} {}".format(sql, self.__group_by.sql())
        if self.__order:
            sql = "{} {}".format(sql, self.__order.sql())
        if self.__limit:
            sql = "{} LIMIT %s,%s".format(sql)
        return sql

    def order_by(self, order):
        self.__order = order
        return self

    def group_by(self, group):
        self.__group_by = group
        return self


class Update(SQLFragment):
    """
    更新数据库
    """

    def __init__(self, table):
        self.table = table
        self.condition = []
        self.cols = []
        self.__args = []

    def where(self, *condition):
        self.condition.extend(condition)
        map(lambda c: self.__args.extend(c.args()), condition)
        return self

    def set(self, *cols):
        self.cols.extend(cols)
        return self

    def sql(self):
        return self.__str__()

    def args(self):
        _args = []
        map(lambda c: _args.extend(c.args()), self.cols)
        map(lambda c: _args.extend(c.args()), self.condition)
        return _args

    def __str__(self):
        sql = "UPDATE `{}`".format(self.table.table_name_)
        cols = ",".join(c.sql() for c in self.cols)
        sql = "{} SET {}".format(sql, cols)
        where = " AND ".join(f.sql() for f in self.condition)
        sql = "{} WHERE {}".format(sql, where)
        return sql


class Insert(SQLFragment):
    """
    插入操作
    """

    def __init__(self, table):
        self.table = table
        self.__enable_keys = set(table.table_columns_())
        if self.table.primary_auto:
            self.__enable_keys.remove(self.table.primary_key.name)
        self.__values = OrderedDict()

    def set(self, *cols):
        for c in cols:
            if c.column.name not in self.__enable_keys:
                raise AttributeError("'{}' object has no attribute {} ".format(self.__class__.__name__, c.column.name))
            self.__values[c.column.name] = c
        return self

    def keys(self):
        return self.__values.keys()

    def args(self):
        r = []
        map(lambda c: r.extend(c.args()), self.__values.values())
        return r

    def sql(self):
        sql = "INSERT INTO `{}` ({}) VALUES ({})"
        al = len(self.__values)
        params = ",".join(map(lambda x_: "`{}`".format(x_), self.__values.keys()))
        return sql.format(self.table.table_name_, params,
                          ",".join(map(lambda _: "%s", range(al))))
