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
