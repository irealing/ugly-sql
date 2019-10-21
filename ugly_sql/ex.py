import logging

__author__ = 'Memory_Leak<irealing@163.com>'


class DBConsole(object):
    """简单数据库操作工具"""

    def __init__(self, connect, args=(), kwargs=None, conn_retry=3):
        self.logger = logging.getLogger(self.__class__.__name__)
        self._connect_fun = connect
        self._connect_args = args
        self._connect_kw = kwargs if kwargs is not None else {}
        self._conn_retry = conn_retry

    def connect(self):
        for x in range(self._conn_retry + 1):
            try:
                return self._connect_fun(*self._connect_args, **self._connect_kw)
            except Exception as e:
                self.logger.exception("connect database exception %s", e.__class__.__name__)
                continue
        raise Exception('connection error after {} time try'.format(self._conn_retry))

    def execute(self, sql, cursor=None, params=(), commit=False):
        """执行SQL语句"""
        params = params if isinstance(params, (tuple, list, set)) else (params,)
        if cursor is None:
            conn = self.connect()
            cursor = conn.cursor()
        else:
            conn = None
        ret = cursor.execute(sql, params)
        if conn and commit:
            conn.commit()
        if conn:
            conn.close()
            cursor.close()
        return ret

    def simple_select(self, sql, cursor=None, params=(), rows=False, callback=None):
        if not cursor:
            conn = self.connect()
            cursor = conn.cursor()
        else:
            conn = None
        try:
            self.execute(sql, cursor=cursor, params=params)
            data = cursor.fetchall() if rows else cursor.fetchone()
            return (data, None) if callback is None else callback(data, None)
        except Exception as e:
            logging.exception("simple_select exception %s", sql)
            return (None, e) if callback is None else callback(None, e)
        finally:
            if conn:
                cursor.close()
                conn.close()

    def simple_update(self, sql, params=(), cursor=None, commit=False, auto_id=False, catch=None):
        """执行更新插入操作"""
        if cursor is None:
            conn = self.connect()
            cursor = conn.cursor()
        else:
            conn = None
        try:
            self.execute(sql, cursor, params, commit=commit)
            if commit and conn:
                conn.commit()
            return cursor.lastrowid if auto_id else cursor.rowcount
        except Exception as e:
            self.logger.exception("simple update exception %s sql %s", sql)
            if not catch:
                raise e
            return catch(e)
        finally:
            if conn:
                cursor.close()
                conn.close()
