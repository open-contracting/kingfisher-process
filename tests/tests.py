import ocdskingfisherprocess.util
from tests.base import BaseTest


class TestDataBase(BaseTest):

    def test_create_tables(self):
        self.setup_main_database()


class TestUtil(BaseTest):

    def test_database_get_hash_md5_for_data(self):
        assert ocdskingfisherprocess.util.get_hash_md5_for_data({'cats': 'many'}) == '538dd075f4a37d77be84c683b711d644'

    def test_database_get_hash_md5_for_data2(self):
        assert ocdskingfisherprocess.util.get_hash_md5_for_data({'cats': 'none'}) == '562c5f4221c75c8f08da103cc10c4e4c'


class TestControlCodes(BaseTest):

    def test_control_code_to_filter_out_to_human_readable(self):
        for control_code_to_filter_out in ocdskingfisherprocess.util.control_codes_to_filter_out:
            # This test just calls it and make sure it runs without crashing
            # (some code was crashing, so wanted test to check all future values of control_codes_to_filter_out)
            print(ocdskingfisherprocess.util.control_code_to_filter_out_to_human_readable(control_code_to_filter_out))
