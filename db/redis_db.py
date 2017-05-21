# coding:utf-8
import datetime
import json
import redis
from config.conf import get_redis_args

redis_args = get_redis_args()


class Cookies(object):
    rd_con = redis.StrictRedis(host=redis_args.get('host'), port=redis_args.get('port'),
                               password=redis_args.get('password'), db=redis_args.get('cookies'))

    @classmethod
    def store_cookies(cls, name, cookies):
        pickled_cookies = json.dumps(
            {'name': name, 'cookies': cookies, 'loginTime': datetime.datetime.now().timestamp()})
        cls.rd_con.lpush('account', pickled_cookies)
        # 为cookie设置过期时间，防止某些账号登录失败，还会获取到失效cookie
        cls.rd_con.expire(name, 20 * 60 * 60)

    @classmethod
    def fetch_cookies(cls):
        for i in range(cls.rd_con.llen('account')):
            j_account = cls.rd_con.rpop('account').decode('utf-8')
            if j_account:
                account = json.loads(j_account)
                loginTime = datetime.datetime.fromtimestamp(account['loginTime'])
                if datetime.datetime.now() - loginTime > datetime.timedelta(hours=20):
                    continue  # 丢弃这个过期账号
                cls.rd_con.lpush('account', j_account)
                return account['name'], json.loads(account['cookies'])
            else:
                return None

    @classmethod
    def delete_cookies(cls, name):
        cls.rd_con.delete(name)
        return True


class Urls(object):
    rd_con = redis.StrictRedis(host=redis_args.get('host'), port=redis_args.get('port'),
                               password=redis_args.get('password'), db=redis_args.get('urls'))

    @classmethod
    def store_crawl_url(cls, url, result):
        cls.rd_con.set(url, result)


class IdNames(object):
    rd_con = redis.StrictRedis(host=redis_args.get('host'), port=redis_args.get('port'),
                               password=redis_args.get('password'), db=redis_args.get('id_name'))

    @classmethod
    def store_id_name(cls, user_name, user_id):
        cls.rd_con.set(user_name, user_id)

    @classmethod
    def fetch_uid_by_name(cls, user_name):
        user_id = cls.rd_con.get(user_name)
        if user_id:
            return user_id.decode('utf-8')
        return ''
