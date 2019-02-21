from ocdskingfisherprocess.checks import Checks


class ProcessQueMessage:

    def __init__(self, database):
        self.database = database

    def process(self, message_as_string, run_until_timestamp=None):
        data_bits = message_as_string.split(" ")
        if data_bits[0] == 'collection-data-store-finished':
            collection = self.database.get_collection(data_bits[1])
            if collection:
                checks = Checks(self.database, collection, run_until_timestamp=run_until_timestamp)
                checks.process_all_files()
