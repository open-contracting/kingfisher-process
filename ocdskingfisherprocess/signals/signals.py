import json
import redis

from ocdskingfisherprocess.signals import KINGFISHER_SIGNALS
from ocdskingfisherprocess.transform import TRANSFORM_TYPE_COMPILE_RELEASES, TRANSFORM_TYPE_UPGRADE_1_0_TO_1_1

# Doing globals this way is hacky. Look into https://www.mattlayman.com/blog/2015/blinker/ instead.
our_database = None
our_config = None


def setup_signals(config, database):
    global our_database, our_config
    our_database = database
    our_config = config
    if config.run_standard_pipeline:
        KINGFISHER_SIGNALS.signal('new_collection_created').connect(run_standard_pipeline_on_new_collection_created)
    if config.is_redis_available():
        KINGFISHER_SIGNALS.signal('collection-data-store-finished').connect(collection_data_store_finished_to_redis)


def run_standard_pipeline_on_new_collection_created(sender, collection_id=None, **kwargs):
    collection = our_database.get_collection(collection_id)
    if not collection.transform_from_collection_id:
        second_collection_id = our_database.get_or_create_collection_id(collection.source_id,
                                                                        collection.data_version,
                                                                        collection.sample,
                                                                        transform_from_collection_id=collection.database_id,
                                                                        transform_type=TRANSFORM_TYPE_UPGRADE_1_0_TO_1_1)

        our_database.get_or_create_collection_id(collection.source_id,
                                                 collection.data_version,
                                                 collection.sample,
                                                 transform_from_collection_id=second_collection_id,
                                                 transform_type=TRANSFORM_TYPE_COMPILE_RELEASES)


def collection_data_store_finished_to_redis(sender, collection_id=None, **kwargs):
    redis_conn = redis.Redis(host=our_config.redis_host, port=our_config.redis_port, db=our_config.redis_database)
    message = json.dumps({'type': 'collection-data-store-finished', 'collection_id': collection_id})
    redis_conn.lpush('kingfisher_work', message)
