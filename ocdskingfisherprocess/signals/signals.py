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
        KINGFISHER_SIGNALS.signal('collection-store-finished').connect(collection_store_finished_to_redis)


def run_standard_pipeline_on_new_collection_created(sender, collection_id=None, ocds_version='1.1', **kwargs):
    global our_database
    collection = our_database.get_collection(collection_id)
    if not collection.transform_from_collection_id:
        if ocds_version == '1.1':
            collection_id_to_compile = collection.database_id
        else:
            # Create the transforms we want
            collection_id_to_compile = our_database.get_or_create_collection_id(
                collection.source_id,
                collection.data_version,
                collection.sample,
                transform_from_collection_id=collection.database_id,
                transform_type=TRANSFORM_TYPE_UPGRADE_1_0_TO_1_1
            )

        our_database.get_or_create_collection_id(collection.source_id,
                                                 collection.data_version,
                                                 collection.sample,
                                                 transform_from_collection_id=collection_id_to_compile,
                                                 transform_type=TRANSFORM_TYPE_COMPILE_RELEASES)

        # Turn on the checks we want
        our_database.mark_collection_check_data(collection_id, True)
        our_database.mark_collection_check_older_data_with_schema_version_1_1(collection_id, True)


def collection_data_store_finished_to_redis(sender,
                                            collection_id=None,
                                            collection_file_item_id=None,
                                            **kwargs):
    redis_conn = redis.Redis(host=our_config.redis_host, port=our_config.redis_port, db=our_config.redis_database)
    message = json.dumps({
        'type': 'collection-data-store-finished',
        'collection_id': collection_id,
        'collection_file_item_id': collection_file_item_id,
    })
    redis_conn.rpush('kingfisher_work', message)


def collection_store_finished_to_redis(sender,
                                       collection_id=None,
                                       **kwargs):
    redis_conn = redis.Redis(host=our_config.redis_host, port=our_config.redis_port, db=our_config.redis_database)
    message = json.dumps({
        'collection_id': collection_id,
    })
    redis_conn.rpush('kingfisher_work_collection_store_finished', message)
