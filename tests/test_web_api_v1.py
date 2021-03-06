import io
import json
import os
import random

import sqlalchemy as sa

from tests.base import BaseWebTest


class TestWebAPIV1(BaseWebTest):

    def test_api_v1_bad_key(self):
        # Call
        data = {
            'collection_source': 'test',
            'collection_data_version': '2018-10-10 00:12:23',
            'collection_sample': 'true',
            'file_name': 'test.json',
            'url': 'http://example.com',
            'data_type': 'record',
            'file': (io.BytesIO(b' {"valid_data": "Totally. It totally is."}'), "data.json")
        }

        not_a_key = str(random.randint(100, 100000000000))
        while not_a_key in self.config.web_api_keys:
            not_a_key = str(random.randint(100, 100000000000))

        result = self.flaskclient.post('/api/v1/submit/file/',
                                       data=data,
                                       content_type='multipart/form-data',
                                       headers={'Authorization': 'ApiKey ' + not_a_key})

        assert result.status_code == 401

    def test_api_v1_no_key(self):
        # Call
        data = {
            'collection_source': 'test',
            'collection_data_version': '2018-10-10 00:12:23',
            'collection_sample': 'true',
            'file_name': 'test.json',
            'url': 'http://example.com',
            'data_type': 'record',
            'file': (io.BytesIO(b' {"valid_data": "Totally. It totally is."}'), "data.json")
        }

        result = self.flaskclient.post('/api/v1/submit/file/',
                                       data=data,
                                       content_type='multipart/form-data')

        assert result.status_code == 401

    def test_api_v1_submit_file(self):
        # Call
        data = {
            'collection_source': 'test',
            'collection_data_version': '2018-10-10 00:12:23',
            'collection_sample': 'true',
            'file_name': 'test.json',
            'url': 'http://example.com',
            'data_type': 'record',
            'file': (io.BytesIO(b' {"valid_data": "Totally. It totally is."}'), "data.json")
        }

        result = self.flaskclient.post('/api/v1/submit/file/',
                                       data=data,
                                       content_type='multipart/form-data',
                                       headers={'Authorization': 'ApiKey ' + self.config.web_api_keys[0]})

        assert result.status_code == 200

        # Check
        collection_id = self.database.get_collection_id('test', '2018-10-10 00:12:23', True)
        assert collection_id

        collection = self.database.get_collection(collection_id)
        assert collection.store_start_at is not None
        assert collection.store_end_at is None

        files = self.database.get_all_files_in_collection(collection_id)
        assert len(files) == 1
        assert files[0].filename == 'test.json'
        assert files[0].url == 'http://example.com'
        assert files[0].errors is None
        assert files[0].warnings is None

        file_items = self.database.get_all_files_items_in_file(files[0])
        assert len(file_items) == 1
        assert file_items[0].errors is None
        assert file_items[0].warnings is None

        notes = self.database.get_all_notes_in_collection(collection_id)
        assert len(notes) == 0

        # Test warnings and errors are empty and actually NULL's in database, for easier finding later
        # https://github.com/open-contracting/kingfisher-process/issues/306
        with self.database.get_engine().begin() as connection:

            collection_file_results = connection.execute(
                'SELECT (warnings is null) as w, (errors is null) as e from collection_file'
            )
            for collection_file_result in collection_file_results:
                assert collection_file_result[0] is True
                assert collection_file_result[1] is True

            collection_file_item_results = connection.execute(
                'SELECT (warnings is null) as w, (errors is null) as e from collection_file_item'
            )
            for collection_file_item_result in collection_file_item_results:
                assert collection_file_item_result[0] is True
                assert collection_file_item_result[1] is True

    def test_api_v1_submit_file_with_control_code(self):
        # Call
        data = {
            'collection_source': 'test',
            'collection_data_version': '2018-10-10 00:12:23',
            'collection_sample': 'true',
            'file_name': 'test.json',
            'url': 'http://example.com',
            'data_type': 'record',
            'file': (io.BytesIO(b' {"control_code": "\\u0000"}'), "data.json")
        }

        result = self.flaskclient.post('/api/v1/submit/file/',
                                       data=data,
                                       content_type='multipart/form-data',
                                       headers={'Authorization': 'ApiKey ' + self.config.web_api_keys[0]})

        assert result.status_code == 200

        # Check
        collection_id = self.database.get_collection_id('test', '2018-10-10 00:12:23', True)
        assert collection_id

        collection = self.database.get_collection(collection_id)
        assert collection.store_start_at is not None
        assert collection.store_end_at is None

        files = self.database.get_all_files_in_collection(collection_id)
        assert len(files) == 1
        assert files[0].filename == 'test.json'
        assert files[0].url == 'http://example.com'
        assert files[0].errors is None
        assert files[0].warnings == ['We had to replace control codes: \\u0000']

        file_items = self.database.get_all_files_items_in_file(files[0])
        assert len(file_items) == 1
        assert file_items[0].errors is None
        assert file_items[0].warnings is None

        notes = self.database.get_all_notes_in_collection(collection_id)
        assert len(notes) == 0

    def test_api_v1_submit_file_that_is_not_even_json(self):
        # Call
        data = {
            'collection_source': 'test',
            'collection_data_version': '2018-10-10 00:12:23',
            'collection_sample': 'true',
            'file_name': 'test.json',
            'url': 'http://example.com',
            'data_type': 'record',
            'file': (io.BytesIO(b'CSV,Files,Are,GREATTTTTTTTTTTTTT'), "data.csv")
        }

        result = self.flaskclient.post('/api/v1/submit/file/',
                                       data=data,
                                       content_type='multipart/form-data',
                                       headers={'Authorization': 'ApiKey ' + self.config.web_api_keys[0]})

        assert result.status_code == 200

        # Check
        collection_id = self.database.get_collection_id('test', '2018-10-10 00:12:23', True)
        assert collection_id

        collection = self.database.get_collection(collection_id)
        assert collection.store_start_at is not None
        assert collection.store_end_at is None

        files = self.database.get_all_files_in_collection(collection_id)
        assert len(files) == 1
        assert files[0].filename == 'test.json'
        assert files[0].url == 'http://example.com'
        assert len(files[0].errors) == 1
        assert files[0].errors[0] == "JSONDecodeError('Expecting value: line 1 column 1 (char 0)',)"
        assert files[0].warnings is None

        file_items = self.database.get_all_files_items_in_file(files[0])
        assert len(file_items) == 0

        notes = self.database.get_all_notes_in_collection(collection_id)
        assert len(notes) == 0

    def test_api_v1_submit_local_file(self):
        # Call
        json_filename = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'fixtures', 'sample_1_0_record.json'
        )

        data = {
            'collection_source': 'test',
            'collection_data_version': '2018-10-10 00:12:23',
            'collection_sample': 'true',
            'file_name': 'test.json',
            'url': 'http://example.com',
            'data_type': 'record',
            'local_file_name': json_filename,
        }

        result = self.flaskclient.post('/api/v1/submit/file/',
                                       data=data,
                                       content_type='multipart/form-data',
                                       headers={'Authorization': 'ApiKey ' + self.config.web_api_keys[0]})

        assert result.status_code == 200

        # Check
        collection_id = self.database.get_collection_id('test', '2018-10-10 00:12:23', True)
        assert collection_id

        collection = self.database.get_collection(collection_id)
        assert collection.store_start_at is not None
        assert collection.store_end_at is None

        files = self.database.get_all_files_in_collection(collection_id)
        assert len(files) == 1
        assert files[0].filename == 'test.json'
        assert files[0].url == 'http://example.com'
        assert files[0].errors is None
        assert files[0].warnings is None

        file_items = self.database.get_all_files_items_in_file(files[0])
        assert len(file_items) == 1
        assert file_items[0].errors is None
        assert file_items[0].warnings is None

        notes = self.database.get_all_notes_in_collection(collection_id)
        assert len(notes) == 0

        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.record_table])
            result = connection.execute(s)
            assert 1 == result.rowcount

            s = sa.sql.select([self.database.release_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

        # Make sure local file exists and was not removed!
        assert os.path.exists(json_filename)

    def test_api_v1_submit_item(self):
        # Call
        data = {
            'collection_source': 'test',
            'collection_data_version': '2018-10-10 00:12:23',
            'collection_sample': 'true',
            'file_name': 'test.json',
            'url': 'http://example.com',
            'data_type': 'record',
            'number': 0,
            'data': ' {"valid_data": "Totally. It totally is."}',
        }

        result = self.flaskclient.post('/api/v1/submit/item/',
                                       data=data,
                                       headers={'Authorization': 'ApiKey ' + self.config.web_api_keys[0]})

        assert result.status_code == 200

        # Check
        collection_id = self.database.get_collection_id('test', '2018-10-10 00:12:23', True)
        assert collection_id

        collection = self.database.get_collection(collection_id)
        assert collection.store_start_at is not None
        assert collection.store_end_at is None

        files = self.database.get_all_files_in_collection(collection_id)
        assert len(files) == 1
        assert files[0].filename == 'test.json'
        assert files[0].url == 'http://example.com'
        assert files[0].errors is None
        assert files[0].warnings is None

        file_items = self.database.get_all_files_items_in_file(files[0])
        assert len(file_items) == 1
        assert file_items[0].errors is None
        assert file_items[0].warnings is None

        notes = self.database.get_all_notes_in_collection(collection_id)
        assert len(notes) == 0

    def test_api_v1_submit_end_collection_store(self):
        # Open collection call
        data = {
            'collection_source': 'test',
            'collection_data_version': '2018-10-10 00:12:23',
            'collection_sample': 'true',
            'file_name': 'test.json',
            'url': 'http://example.com',
            'data_type': 'record',
            'file': (io.BytesIO(b' {"valid_data": "Totally. It totally is."}'), "data.json")
        }

        result = self.flaskclient.post('/api/v1/submit/file/',
                                       data=data,
                                       content_type='multipart/form-data',
                                       headers={'Authorization': 'ApiKey ' + self.config.web_api_keys[0]})

        assert result.status_code == 200

        # End Collection call
        data = {
            'collection_source': 'test',
            'collection_data_version': '2018-10-10 00:12:23',
            'collection_sample': 'true',
        }

        result = self.flaskclient.post('/api/v1/submit/end_collection_store/',
                                       data=data,
                                       headers={'Authorization': 'ApiKey ' + self.config.web_api_keys[0]})

        assert result.status_code == 200

        # Check
        collection_id = self.database.get_collection_id('test', '2018-10-10 00:12:23', True)
        assert collection_id

        collection = self.database.get_collection(collection_id)
        assert collection.store_start_at is not None
        assert collection.store_end_at is not None

        notes = self.database.get_all_notes_in_collection(collection_id)
        assert len(notes) == 0

    def test_api_v1_submit_file_errors(self):
        # Call
        data = {
            'collection_source': 'test',
            'collection_data_version': '2018-10-10 00:12:23',
            'collection_sample': 'true',
            'file_name': 'test.json',
            'url': 'http://example.com',
            'errors': json.dumps(['The error was ... because reasons.'])
        }

        result = self.flaskclient.post('/api/v1/submit/file_errors/',
                                       data=data,
                                       content_type='multipart/form-data',
                                       headers={'Authorization': 'ApiKey ' + self.config.web_api_keys[0]})

        assert result.status_code == 200

        # Check
        collection_id = self.database.get_collection_id('test', '2018-10-10 00:12:23', True)
        assert collection_id

        collection = self.database.get_collection(collection_id)
        assert collection.store_start_at is not None
        assert collection.store_end_at is None

        files = self.database.get_all_files_in_collection(collection_id)
        assert len(files) == 1
        assert files[0].filename == 'test.json'
        assert files[0].url == 'http://example.com'
        assert len(files[0].errors) == 1
        assert files[0].errors[0] == 'The error was ... because reasons.'
        assert files[0].warnings is None

        file_items = self.database.get_all_files_items_in_file(files[0])
        assert len(file_items) == 0

        notes = self.database.get_all_notes_in_collection(collection_id)
        assert len(notes) == 0

    def test_api_v1_submit_item_nothing(self):
        # Call
        data = {
        }

        result = self.flaskclient.post('/api/v1/submit/item/',
                                       data=data,
                                       headers={'Authorization': 'ApiKey ' + self.config.web_api_keys[0]})

        assert result.status_code == 400

    def test_api_v1_submit_item_no_release_key(self):
        # Call
        data = {
            'collection_source': 'test',
            'collection_data_version': '2018-10-10 00:12:23',
            'collection_sample': 'true',
            'file_name': 'test.json',
            'url': 'http://example.com',
            'data_type': 'release_package',
            'number': 0,
            'data': ' {"missing": "A release key. Seen in real data. So we want to make sure we test for that."}',
        }

        result = self.flaskclient.post('/api/v1/submit/item/',
                                       data=data,
                                       headers={'Authorization': 'ApiKey ' + self.config.web_api_keys[0]})

        assert result.status_code == 200

        # Check
        collection_id = self.database.get_collection_id('test', '2018-10-10 00:12:23', True)
        assert collection_id

        collection = self.database.get_collection(collection_id)
        assert collection.store_start_at is not None
        assert collection.store_end_at is None

        files = self.database.get_all_files_in_collection(collection_id)
        assert len(files) == 1
        assert files[0].filename == 'test.json'
        assert files[0].url == 'http://example.com'
        assert files[0].errors is None
        assert files[0].warnings is None

        file_items = self.database.get_all_files_items_in_file(files[0])
        assert len(file_items) == 1
        assert file_items[0].errors == ['Release list not found']
        assert file_items[0].warnings is None

        notes = self.database.get_all_notes_in_collection(collection_id)
        assert len(notes) == 0

    def test_api_v1_submit_item_no_release_key_succesfull_call_first(self, ):

        # Call - good data
        json_filename = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'fixtures', 'sample_1_0_releases.json'
        )

        with open(json_filename) as f:
            data = {
                'collection_source': 'test',
                'collection_data_version': '2018-10-10 00:12:23',
                'collection_sample': 'true',
                'file_name': 'test.json',
                'url': 'http://example.com',
                'data_type': 'release_package',
                'number': 0,
                'data': f.read(),
            }

        result = self.flaskclient.post('/api/v1/submit/item/',
                                       data=data,
                                       headers={'Authorization': 'ApiKey ' + self.config.web_api_keys[0]})

        assert result.status_code == 200

        # Call - bad data
        data = {
            'collection_source': 'test',
            'collection_data_version': '2018-10-10 00:12:23',
            'collection_sample': 'true',
            'file_name': 'test.json',
            'url': 'http://example.com',
            'data_type': 'release_package',
            'number': 1,
            'data': ' {"missing": "A release key. Seen in real data. So we want to make sure we test for that."}',
        }

        result = self.flaskclient.post('/api/v1/submit/item/',
                                       data=data,
                                       headers={'Authorization': 'ApiKey ' + self.config.web_api_keys[0]})

        assert result.status_code == 200

        # Check
        collection_id = self.database.get_collection_id('test', '2018-10-10 00:12:23', True)
        assert collection_id

        collection = self.database.get_collection(collection_id)
        assert collection.store_start_at is not None
        assert collection.store_end_at is None

        files = self.database.get_all_files_in_collection(collection_id)
        assert len(files) == 1
        assert files[0].filename == 'test.json'
        assert files[0].url == 'http://example.com'
        assert files[0].errors is None
        assert files[0].warnings is None

        file_items = self.database.get_all_files_items_in_file(files[0])
        assert len(file_items) == 2
        assert file_items[0].errors is None
        assert file_items[0].warnings is None
        assert file_items[1].errors == ['Release list not found']
        assert file_items[1].warnings is None

        notes = self.database.get_all_notes_in_collection(collection_id)
        assert len(notes) == 0
