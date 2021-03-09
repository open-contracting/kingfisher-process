import logging

from ocdskingfisherprocess.store import Store


class BaseTransform():

    def __init__(self, config, database, destination_collection, run_until_timestamp=None):
        self.config = config
        self.database = database
        self.destination_collection = destination_collection
        if destination_collection.transform_from_collection_id:
            self.source_collection = self.database.get_collection(destination_collection.transform_from_collection_id)
        else:
            self.source_collection = None
        self.store = Store(config, database)
        self.store.set_collection(destination_collection)
        self.run_until_timestamp = run_until_timestamp
        self.logger = logging.getLogger('ocdskingfisher.transform')

    def process(self):
        # This is an "abstract" method - child classes should implement it!
        pass
