import os
import redis
from rq import Worker, Queue, Connection
from bridge_flask_rq import create_flow_pipeline



def main_worker():
    listen = ['default']
    server_ip = '10.2.42.83'
    port = '7123'
    url = 'redis://{server_ip}:{port}'.format(server_ip=server_ip,port=port)
    redis_url = os.getenv('REDISTOGO_URL', url)
    conn = redis.from_url(redis_url)
    with Connection(conn):
        worker = Worker(list(map(Queue, listen)))
        worker.work()
if __name__ == '__main__':
    main_worker()
