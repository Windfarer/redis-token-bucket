import redis
import logging
import time


class TokenBucketManager(object):
    def __init__(self, redis_url=None, default_rate=5, default_burst=5):
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
        self._default_burst = default_burst
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

    def _create_bucket(self, key, rate=None, burst=None):
        logging.debug("create bucket")
        ts = time.time()
        if not burst:
            burst = self._default_burst
        if not rate:
            rate = self._default_rate
        tk = min(rate, burst) - 1
        self._redis_conn.pipeline()\
            .hset(key, "tk", tk)\
            .hset(key, "ts", ts)\
            .hset(key, "bst", burst).execute()
        return tk

    def _check_and_refill(self, key, rate=None, burst=None):
        logging.debug("check and refill")
        last_refill = float(self._redis_conn.hget(key, "ts"))
        n = int((time.time() - last_refill) / 1)
        if n > 0:
            bst = self._redis_conn.hget(key, "bst")
            if not bst:
                if burst:
                    bst = burst
                else:
                    bst = self._default_burst
                self._redis_conn.hset(key, "bst", burst)
            bst = int(bst)
            if not rate:
                rate = self._default_rate
            tk = min(rate * n, bst) - 1
            self._redis_conn.pipeline()\
                .hset(key, "tk", tk)\
                .hset(key, "ts", time.time())
            logging.debug(self._redis_conn.hget(key, "tk"))
            return tk
        else:
            return -1

