from ocdskingfisherprocess.transform import TRANSFORM_TYPE_COMPILE_RELEASES, TRANSFORM_TYPE_UPGRADE_1_0_TO_1_1
from ocdskingfisherprocess.signals import KINGFISHER_SIGNALS


our_database = None


def setup_signals(config, database):
    global our_database
    our_database = database
    if config.run_standard_pipeline:
        KINGFISHER_SIGNALS.signal('new_collection_created').connect(run_standard_pipeline_on_new_collection_created)


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
