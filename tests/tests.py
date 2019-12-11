import ocdskingfisherprocess.util
from tests.base import BaseTest, BaseDataBaseTest


class TestDataBase(BaseDataBaseTest):
    def test_get_collection_id_and_get_or_create_collection_id(self):
        id = self.database.get_collection_id("test-source", "2019-01-20 10:00:12", False)
        # Doesn't exist, so ...
        assert not id

        create_id = self.database.get_or_create_collection_id("test-source", "2019-01-20 10:00:12", False)
        # Now it exists ...
        assert create_id

        get1_id = self.database.get_collection_id("test-source", "2019-01-20 10:00:12", False)
        # And we can load it using get!
        assert get1_id == create_id

        get2_id = self.database.get_or_create_collection_id("test-source", "2019-01-20 10:00:12", False)
        # And we can load it using get or create!
        assert get2_id == create_id


class TestUtil(BaseTest):
    def test_database_get_hash_md5_for_data(self):
        assert ocdskingfisherprocess.util.get_hash_md5_for_data({'cats': 'many'}) == '538dd075f4a37d77be84c683b711d644'

    def test_database_get_hash_md5_for_data2(self):
        assert ocdskingfisherprocess.util.get_hash_md5_for_data({'cats': 'none'}) == '562c5f4221c75c8f08da103cc10c4e4c'


class TestControlCodes1(BaseTest):
    def test_control_code_to_filter_out_to_human_readable(self):
        for control_code_to_filter_out in ocdskingfisherprocess.util.control_codes_to_filter_out:
            # This test just calls it and make sure it runs without crashing
            # (some code was crashing, so wanted test to check all future values of control_codes_to_filter_out)
            print(ocdskingfisherprocess.util.control_code_to_filter_out_to_human_readable(control_code_to_filter_out))


class TestControlCodes2(BaseDataBaseTest):
    def test_bad_data_with_control_codes(self):
        source_collection_id, source_collection = self.get_collection_and_store_file(
            'sample_1_0_record_with_control_codes.json', 'record')

        # Check Warnings
        with self.database.get_engine().begin() as connection:
            result = self.assert_row_count(connection, 'collection_file', 1)
            data = result.fetchone()
            assert len(data['warnings']) == 1
            assert data['warnings'][0] == 'We had to replace control codes: chr(16)'
