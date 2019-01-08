import json
from ocdskingfisherprocess.database import DatabaseStore


class Store:

    def __init__(self, config, database):
        self.config = config
        self.collection_id = None
        self.database = database

    def load_collection(self, collection_source, collection_data_version, collection_sample):
        self.collection_id = self.database.get_or_create_collection_id(collection_source, collection_data_version, collection_sample)

    def store_file_from_local(self, filename, url, data_type, encoding, local_filename):

        if data_type == 'release_package_json_lines' or data_type == 'record_package_json_lines':
            try:
                with open(local_filename, encoding=encoding) as f:
                    number = 0
                    raw_data = f.readline()
                    while raw_data:
                        self.store_file_item(filename, url, data_type, json.loads(raw_data), number)
                        raw_data = f.readline()
                        number += 1
            except Exception as e:
                raise e
                # TODO Store error in database and make nice HTTP response!

        else:
            try:
                with open(local_filename, encoding=encoding) as f:
                    data = json.load(f)

            except Exception as e:
                raise e
                # TODO Store error in database and make nice HTTP response!

            objects_list = []
            if data_type == 'record_package_list_in_results':
                objects_list.extend(data['results'])
            elif data_type == 'release_package_list_in_results':
                objects_list.extend(data['results'])
            elif data_type == 'record_package_list' or data_type == 'release_package_list':
                objects_list.extend(data)
            else:
                objects_list.append(data)

            number = 0
            for item_data in objects_list:

                try:
                    self.store_file_item(filename, url, data_type, item_data, number)
                    number += 1

                except Exception as e:
                    raise e
                    # TODO Store error in database and make nice HTTP response!

        self.database.mark_collection_file_store_done(self.collection_id, filename)

    def store_file_item_from_local(self, filename, url, data_type, encoding, number, local_filename):

        try:
            with open(local_filename, encoding=encoding) as f:
                data = json.load(f)

        except Exception as e:
            raise e
            # TODO Store error in database and make nice HTTP response!

        try:
            self.store_file_item(filename, url, data_type, data, number)

        except Exception as e:
            raise e
            # TODO Store error in database and make nice HTTP response!

    def store_file_item(self, filename, url, data_type, json_data, number):

        if not isinstance(json_data, dict):
            raise Exception("Can not process data as JSON is not an object")

        with DatabaseStore(database=self.database, collection_id=self.collection_id, file_name=filename, number=number) as store:

            if data_type == 'release' or data_type == 'record':
                data_list = [json_data]
            elif data_type == 'release_package' or \
                    data_type == 'release_package_json_lines' or \
                    data_type == 'release_package_list_in_results' or \
                    data_type == 'release_package_list':
                if 'releases' not in json_data:
                    if data_type == 'release_package_json_lines' and \
                            self.ignore_release_package_json_lines_missing_releases_error:
                        return
                    raise Exception("Release list not found")
                elif not isinstance(json_data['releases'], list):
                    raise Exception("Release list which is not a list found")
                data_list = json_data['releases']
            elif data_type == 'record_package' or \
                    data_type == 'record_package_json_lines' or \
                    data_type == 'record_package_list_in_results' or \
                    data_type == 'record_package_list':
                if 'records' not in json_data:
                    raise Exception("Record list not found")
                elif not isinstance(json_data['records'], list):
                    raise Exception("Record list which is not a list found")
                data_list = json_data['records']
            else:
                raise Exception("data_type not a known type")

            package_data = {}
            if not data_type == 'release':
                for key, value in json_data.items():
                    if key not in ('releases', 'records'):
                        package_data[key] = value

            for row in data_list:
                if not isinstance(row, dict):
                    raise Exception("Row in data is not a object")

                if data_type == 'record' or \
                        data_type == 'record_package' or \
                        data_type == 'record_package_json_lines' or \
                        data_type == 'record_package_list_in_results' or \
                        data_type == 'record_package_list':
                    store.insert_record(row, package_data)
                else:
                    store.insert_release(row, package_data)
