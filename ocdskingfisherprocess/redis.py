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
                # Older messages might not have the extra data in, so we need to check for this.
                if 'collection_file_item_id' in message_as_data and message_as_data['collection_file_item_id']:
                    checks.process_file_item_id(message_as_data['collection_file_item_id'])
                else:
                    checks.process_all_files()
