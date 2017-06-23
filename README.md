# redis-token-bucket

## Usage:
```python
from redis_token_bucket import TokenBucketManager

tbm = TokenBucketManager(redis_url="redis://path-to-your-redis", default_rate=10)
# default_rate means how many tokens will be generated per second. 

ok = tbm.get_token("my_key")
# if token avaliable, return True, else return False

```

## TODO
* burst rate