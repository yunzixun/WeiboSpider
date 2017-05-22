import sys

sys.path.append('/'.join(sys.path[0].split('/')))

from db.redis_db import Cookies
print("刷新账号队列")
Cookies.fresh_login_queue(10)
