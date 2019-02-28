import io

from tests.base import BaseWebTest


class TestWebAPIV1CollectionNotes(BaseWebTest):

    def test_api_v1_submit_file(self):
        # Call First Time!
        data = {
            'collection_source': 'test',
            'collection_data_version': '2018-10-10 00:12:23',
            'collection_sample': 'true',
            'file_name': 'test1.json',
            'url': 'http://example.com',
            'data_type': 'record',
            'collection_note': '            Test First Note!!! ',
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

        notes = self.database.get_all_notes_in_collection(collection_id)
        assert len(notes) == 1
        assert notes[0].note == 'Test First Note!!!'

        # Call Second Time! With a different Note
        data = {
            'collection_source': 'test',
            'collection_data_version': '2018-10-10 00:12:23',
            'collection_sample': 'true',
            'file_name': 'test2.json',
            'url': 'http://example.com',
            'data_type': 'record',
            'collection_note': '            Twins! ',
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

        notes = self.database.get_all_notes_in_collection(collection_id)
        assert len(notes) == 2
        assert notes[0].note == 'Test First Note!!!'
        assert notes[1].note == 'Twins!'

        # Call Third Time! With the same Note - so should not be saved again
        data = {
            'collection_source': 'test',
            'collection_data_version': '2018-10-10 00:12:23',
            'collection_sample': 'true',
            'file_name': 'test3.json',
            'url': 'http://example.com',
            'data_type': 'record',
            'collection_note': '            Twins! ',
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

        notes = self.database.get_all_notes_in_collection(collection_id)
        assert len(notes) == 2
        assert notes[0].note == 'Test First Note!!!'
        assert notes[1].note == 'Twins!'

    def test_api_v1_submit_item(self):
        # Call First Time!
        data = {
            'collection_source': 'test',
            'collection_data_version': '2018-10-10 00:12:23',
            'collection_sample': 'true',
            'file_name': 'test.json',
            'url': 'http://example.com',
            'data_type': 'record',
            'number': 0,
            'collection_note': '            Test First Note!!! ',
            'data': ' {"valid_data": "Totally. It totally is."}',
        }

        result = self.flaskclient.post('/api/v1/submit/item/',
                                       data=data,
                                       headers={'Authorization': 'ApiKey ' + self.config.web_api_keys[0]})

        assert result.status_code == 200

        # Check
        collection_id = self.database.get_collection_id('test', '2018-10-10 00:12:23', True)
        assert collection_id

        notes = self.database.get_all_notes_in_collection(collection_id)
        assert len(notes) == 1
        assert notes[0].note == 'Test First Note!!!'

        # Call Second Time! With a different Note
        data = {
            'collection_source': 'test',
            'collection_data_version': '2018-10-10 00:12:23',
            'collection_sample': 'true',
            'file_name': 'test.json',
            'url': 'http://example.com',
            'data_type': 'record',
            'number': 1,
            'collection_note': '    Twins! ',
            'data': ' {"valid_data": "Totally. It totally is."}',
        }

        result = self.flaskclient.post('/api/v1/submit/item/',
                                       data=data,
                                       headers={'Authorization': 'ApiKey ' + self.config.web_api_keys[0]})

        assert result.status_code == 200

        # Check
        collection_id = self.database.get_collection_id('test', '2018-10-10 00:12:23', True)
        assert collection_id

        notes = self.database.get_all_notes_in_collection(collection_id)
        assert len(notes) == 2
        assert notes[0].note == 'Test First Note!!!'
        assert notes[1].note == 'Twins!'

        # Call Third Time! With the same Note - so should not be saved again
        data = {
            'collection_source': 'test',
            'collection_data_version': '2018-10-10 00:12:23',
            'collection_sample': 'true',
            'file_name': 'test.json',
            'url': 'http://example.com',
            'data_type': 'record',
            'number': 2,
            'collection_note': 'Twins!            ',
            'data': ' {"valid_data": "Totally. It totally is."}',
        }

        result = self.flaskclient.post('/api/v1/submit/item/',
                                       data=data,
                                       headers={'Authorization': 'ApiKey ' + self.config.web_api_keys[0]})

        assert result.status_code == 200

        # Check
        collection_id = self.database.get_collection_id('test', '2018-10-10 00:12:23', True)
        assert collection_id

        notes = self.database.get_all_notes_in_collection(collection_id)
        assert len(notes) == 2
        assert notes[0].note == 'Test First Note!!!'
        assert notes[1].note == 'Twins!'
