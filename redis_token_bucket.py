import redis
import logging
import time


class TokenBucketManager(object):
    def __init__(self, redis_url=None, default_rate=5):
        if redis_url is None:
            redis_url = "redis://localhost:6379"
        self._redis_conn = redis.StrictRedis.from_url(url=redis_url)
        ok = True
        try:
            ok = self._redis_conn.ping()
        except redis.ConnectionError:
            logging.warning("Redis is not available, TokenBucket will not work")
            self._redis_conn = None
        if not ok:
            self._redis_conn = None
        self._default_rate = default_rate
        self._default_init_rate = default_rate - 1  # use one token after init

    def get_token(self, key):
        if not self._redis_conn:
            logging.warning("Redis is not available, return True")
            return True
        tk = self._redis_conn.hget(key, "tk")
        if tk is None:
            tk = self._create_bucket(key)
        else:
            tk = int(tk)
        if tk < 1:
            tk = self._check_and_refill(key)
            if tk < 0:
                return False
        else:
            self._redis_conn.hset(key, "tk", tk - 1)
        logging.debug(self._redis_conn.hget(key, "tk"))
        return True

    def _create_bucket(self, key):
        logging.debug("create bucket")
        tk = self._default_init_rate
        ts = time.time()
        self._redis_conn.pipeline().hset(key, "tk", tk).hset(key, "ts", ts).execute()
        return tk


    def _check_and_refill(self, key):
        logging.debug("check and refill")
        last_refill = float(self._redis_conn.hget(key, "ts"))
        if last_refill + 1 < time.time():
            tk = self._default_init_rate
            self._redis_conn.pipeline().hset(key, "tk", tk).hset(key, "ts", time.time()).execute()
            return tk
        else:
            return -1
