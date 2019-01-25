from tests.base import BaseWebTest
import io


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
