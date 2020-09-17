import redis
from prometheus_client import Gauge

PROMETHEUS_REDIS_QUEUE_LENGTH = Gauge(
    'kingfisher_process_redis_queue_length',
    'Length of Redis Que'
)
PROMETHEUS_REDIS_QUEUE_COLLECTION_STORE_FINISHED_LENGTH = Gauge(
    'kingfisher_process_redis_queue_collection_store_finished_length',
    'Length of Redis Que for Collection Store Finished Events'
)


def update_all_prometheus_stats(config):
    if config.is_redis_available():
        redis_conn = redis.Redis(host=config.redis_host, port=config.redis_port, db=config.redis_database)
        PROMETHEUS_REDIS_QUEUE_LENGTH.set(
            redis_conn.llen('kingfisher_work')
        )
        PROMETHEUS_REDIS_QUEUE_COLLECTION_STORE_FINISHED_LENGTH.set(
            redis_conn.llen('kingfisher_work_collection_store_finished')
        )
