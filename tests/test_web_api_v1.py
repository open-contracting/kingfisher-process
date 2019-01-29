from tests.base import BaseWebTest
import sqlalchemy as sa
import io
import os


class TestWebAPIV1(BaseWebTest):

    def test_api_v1_submit_file(self):
        self.setup_main_database()

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

        collection_id = self.database.get_collection_id('test', '2018-10-10 00:12:23', True)
        assert collection_id

        files = self.database.get_all_files_in_collection(collection_id)
        assert len(files) == 1
        assert files[0].filename == 'test.json'

    def test_api_v1_submit_local_file(self):
        self.setup_main_database()

        json_filename = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'data', 'sample_1_0_record.json'
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

        collection_id = self.database.get_collection_id('test', '2018-10-10 00:12:23', True)
        assert collection_id

        files = self.database.get_all_files_in_collection(collection_id)
        assert len(files) == 1
        assert files[0].filename == 'test.json'

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
        self.setup_main_database()

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

        collection_id = self.database.get_collection_id('test', '2018-10-10 00:12:23', True)
        assert collection_id

        files = self.database.get_all_files_in_collection(collection_id)
        assert len(files) == 1
        assert files[0].filename == 'test.json'
