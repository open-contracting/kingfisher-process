import json

from ocdskingfisherprocess.checks import Checks


class ProcessQueueMessage:

    def __init__(self, database):
        self.database = database

    def process(self, message_as_string, run_until_timestamp=None):
        message_as_data = json.loads(message_as_string)
        if message_as_data['type'] == 'collection-data-store-finished':
            collection = self.database.get_collection(message_as_data['collection_id'])
            if collection:
                checks = Checks(self.database, collection, run_until_timestamp=run_until_timestamp)
                checks.process_all_files()
