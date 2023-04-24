import redis
import pickle
from config import REDIS_PORT, REDIS_HOST


# Func for add data to Redis DB
def add_data_to_redis(table: str, data: dict | list, db: int):
    try:
        redis_con = redis.from_url(url=f'redis://{REDIS_HOST}:{REDIS_PORT}/{db}')
        redis_con.set(table, pickle.dumps(data))
        redis_con.close()
    except Exception as ex:
        return f"Exception with add data to REDIS {ex}"


# Func for get data from Redis DB
def get_data(table: str, db: int):
    redis_con = redis.from_url(url=f'redis://{REDIS_HOST}:{REDIS_PORT}/{db}')

    data = redis_con.get(table)
    if data:
        data = pickle.loads(data)
        return data
    return {}
