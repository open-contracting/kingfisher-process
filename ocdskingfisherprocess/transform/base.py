from ocdskingfisherprocess.store import Store


class BaseTransform():

    def __init__(self, config, database, destination_collection):
        self.config = config
        self.database = database
        self.destination_collection = destination_collection
        self.source_collection = self.database.get_collection(destination_collection.transform_from_collection_id)
        self.store = Store(config, database)
        self.store.set_collection(destination_collection)

    def process(self):
        # This is an "abstract" method - child classes should implement it!
        pass
