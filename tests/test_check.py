from ocdskingfisherprocess.checks import Checks
from tests.base import BaseDataBaseTest


class BaseTest(BaseDataBaseTest):
    """
    The base class for tests of the Checks class.

    Set a ``count`` class attribute for the number of rows that are expected to be added to the table.

    If you want to perform an additional test on the first added row, define a ``check`` static method that accepts the
    row as its only argument and returns a boolean.

    If you want to test using ``process_file_item_id`` instead of ``process_all_files``, define a ``file_item``
    instance method that accepts a collection ID as its only argument and returns a file item.
    """
    def alter_config(self):
        self.config.run_standard_pipeline = False

    def test_records(self):
        self.run('record_check', 'sample_1_0_record.json', 'record')

    def test_releases(self):
        self.run('release_check', 'sample_1_0_release.json', 'release_package')

    def run(self, table, filename, data_type):
        collection_id, collection = self.get_collection_and_store_file(filename, data_type)

        # Check that no files are processed yet.
        self._check_number_of_check_results()

        # Check that the files are processed as expected.
        self._process_files(collection, collection_id, table)

        # Running another time should have no effect.
        if self.count:
            self._process_files(collection, collection_id, table)

    def _process_files(self, collection, collection_id, table):
        checks = Checks(self.database, collection)

        if hasattr(self, 'file_item'):
            checks.process_file_item_id(self.file_item(collection_id).database_id)
        else:
            checks.process_all_files()

        self._check_number_of_check_results(table)

    def _check_number_of_check_results(self, table=None):
        with self.database.get_engine().begin() as connection:
            for t in ('record_check', 'release_check', 'record_check_error', 'release_check_error'):
                result = self.assert_row_count(connection, t, self.count if t == table else 0)
                if t == table and hasattr(self, 'check'):
                    data = result.fetchone()
                    assert self.check(data)


class TestAllChecksOff(BaseTest):
    count = 0


class TestCheckOn(BaseTest):
    count = 1

    def setup_collection(self, collection_id):
        self.database.mark_collection_check_data(collection_id, True)

    @staticmethod
    def check(data):
        return not data.override_schema_version


class TestCheckOlderThan11On(BaseTest):
    count = 1

    def setup_collection(self, collection_id):
        self.database.mark_collection_check_older_data_with_schema_version_1_1(collection_id, True)

    @staticmethod
    def check(data):
        return '1.1' == data.override_schema_version


class TestCheckAllOn(BaseTest):
    count = 2

    def setup_collection(self, collection_id):
        self.database.mark_collection_check_data(collection_id, True)
        self.database.mark_collection_check_older_data_with_schema_version_1_1(collection_id, True)


class TestCheckAllOnFileItem(TestCheckAllOn):  # Unlike others, inherits from TestCheckAllOn.
    def file_item(self, collection_id):
        file = self.database.get_all_files_in_collection(collection_id)[0]
        return self.database.get_all_files_items_in_file(file)[0]


class TestCheck11NotSelected(BaseDataBaseTest):
    def alter_config(self):
        self.config.run_standard_pipeline = False

    def test_records_via_process_all_files_method(self):
        collection_id, collection = self.get_collection_and_store_file(
            'sample_1_1_record.json', 'record')

        assert len([r for r in self.database.get_records_to_check(collection_id, override_schema_version='1.1')]) == 0
        assert len([r for r in self.database.get_records_to_check(collection_id, override_schema_version='1.2')]) == 1

    def test_releases_via_process_all_files_method(self):
        collection_id, collection = self.get_collection_and_store_file(
            'sample_1_1_releases_multiple_with_same_ocid.json', 'release_package')

        assert len([r for r in self.database.get_releases_to_check(collection_id, override_schema_version='1.1')]) == 0
        assert len([r for r in self.database.get_releases_to_check(collection_id, override_schema_version='1.2')]) == 6
