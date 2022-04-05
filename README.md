## ugly-sql

### 基础使用

数据库连接工具:
```python
import pymysql
from ugly_sql import SessionManager
import logging
import sys
import contextlib

@contextlib.contextmanager
def db_ctx(cfg,tran=False):
    conn=pymysql.connect(**cfg)
    sm=SessionManager(conn)
    try:
        if tran:
            sm.begin()
        yield sm.__enter__()
        sm.__exit__(None,None,None)
    except Exception as e:
        logging.warning("db_ctx error %s",e)
        et,ev,tb=sys.exc_info()
        sm.__exit__(et,ev,tb)
        raise e
    finally:
        conn.close()
        
```
基础操作:

```python
from ugly_sql import Table
User = Table("users",("id","name","password"),"id",True)
db_cfg={}
if __name__ == '__main__':
    with db_ctx(db_cfg,True) as db:
        user=db.query(User).filter(User.name=="root",User.password=="***").one()
        print("name= {}\n password= {}".format(user.name,user.password))
```

### 扩展工具

#### DBConsole[简单数据库操作工具(SQL方式)]

* ugly_sql.ex.DBConsole(self, connect, args=(), kwargs=None, conn_retry=3)

  ​	connect: 创建数据库连接的函数

  ​    args: 创建数据库连接的参数

  ​	kwargs: 创建数据库连接的命名参数

  ​	conn_retry: 创建连接异常的重试次数

```python
import pymysql

from ugly_sql.ex import DBConsole
cfg=dict(
    host='127.0.0.1', port=3306, user='***', passwd='***', charset='utf8',
    db='test'
)
db = DBConsole(pymysql.connect,kwargs=cfg)
```

* simple_select(sql, cursor=None, params=(), rows=False, callback=None)

  sql: 执行的SQL语句

  params: SQL语句的参数

  rows: 指示是否返回多条数据

  callback: 处理数据的回调函数，callback(data,exception)

  ```python
  db.simple_select("select * from application",callback=lambda d,e:d,rows=True)
  ```

* simple_update(sql, params=(), cursor=None, commit=False, auto_id=False, catch=None)

  sql: 执行的SQL语句

  param: SQL语句的参数

  commit: 是否commit

  auto_id:自动生成的ID，auto_id为True则返回ID值

  catch: 发生异常时的回调catch(exception),未指定则抛出异常

```json
db.simple_update("update application set enable=1 where id=%s", params=1, catch=lambda e: print(e))
```

