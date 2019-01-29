from ocdskingfisherprocess.store import Store


class BaseTransform():

    def __init__(self, config, database, destination_collection, run_until_timestamp=None):
        self.config = config
        self.database = database
        self.destination_collection = destination_collection
        self.source_collection = self.database.get_collection(destination_collection.transform_from_collection_id)
        self.store = Store(config, database)
        self.store.set_collection(destination_collection)
        self.run_until_timestamp = run_until_timestamp

    def process(self):
        # This is an "abstract" method - child classes should implement it!
        pass
