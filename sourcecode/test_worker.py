import os
import redis
from rq import Worker, Queue, Connection
from function_tmp import create_flow_pipeline

listen = ['default']
redis_url = os.getenv('REDISTOGO_URL', 'redis://10.2.42.83:7123')
conn = redis.from_url(redis_url)


if __name__ == '__main__':
    with Connection(conn):
        worker = Worker(list(map(Queue, listen)))
        worker.work()
