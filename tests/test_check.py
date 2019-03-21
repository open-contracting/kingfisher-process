import datetime
import os
import sqlalchemy as sa
from ocdskingfisherprocess.store import Store
from tests.base import BaseDataBaseTest
from ocdskingfisherprocess.checks import Checks


class TestDefaultsOff(BaseDataBaseTest):

    def alter_config(self):
        self.config.default_value_collection_check_data = False
        self.config.default_value_collection_check_older_data_with_schema_version_1_1 = False

    def test_records(self):

        collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)
        collection = self.database.get_collection(collection_id)

        store = Store(self.config, self.database)
        store.set_collection(collection)

        json_filename = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'data', 'sample_1_0_record.json'
        )

        store.store_file_from_local("test.json", "http://example.com", "record", "utf-8", json_filename)

        # Check Number of check results
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.record_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.record_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

        # Call Checks
        checks = Checks(self.database, collection)
        checks.process_all_files()

        # Check Number of check results
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.record_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.record_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

    def test_releases(self):

        self.config.default_value_collection_check_data = False
        self.config.default_value_collection_check_older_data_with_schema_version_1_1 = False

        collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)
        collection = self.database.get_collection(collection_id)

        store = Store(self.config, self.database)
        store.set_collection(collection)

        json_filename = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'data', 'sample_1_0_release.json'
        )

        store.store_file_from_local("test.json", "http://example.com", "release_package", "utf-8", json_filename)

        # Check Number of check results
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.record_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.record_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

        # Call Checks
        checks = Checks(self.database, collection)
        checks.process_all_files()

        # Check Number of check results
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.record_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.record_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount


class TestCheckOn(BaseDataBaseTest):

    def alter_config(self):
        self.config.default_value_collection_check_data = True
        self.config.default_value_collection_check_older_data_with_schema_version_1_1 = False

    def test_records(self):

        collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)
        collection = self.database.get_collection(collection_id)

        store = Store(self.config, self.database)
        store.set_collection(collection)

        json_filename = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'data', 'sample_1_0_record.json'
        )

        store.store_file_from_local("test.json", "http://example.com", "record", "utf-8", json_filename)

        # Check Number of check results
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.record_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.record_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

        # Call Checks
        checks = Checks(self.database, collection)
        checks.process_all_files()

        # Check Number of check results
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.record_check_table])
            result = connection.execute(s)
            assert 1 == result.rowcount
            data = result.fetchone()
            assert not data.override_schema_version

            s = sa.sql.select([self.database.release_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.record_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

        # Call Checks Again - that should be fine
        checks = Checks(self.database, collection)
        checks.process_all_files()

        # Check Number of check results
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.record_check_table])
            result = connection.execute(s)
            assert 1 == result.rowcount
            data = result.fetchone()
            assert not data.override_schema_version

            s = sa.sql.select([self.database.release_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.record_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

    def test_release(self):

        collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)
        collection = self.database.get_collection(collection_id)

        store = Store(self.config, self.database)
        store.set_collection(collection)

        json_filename = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'data', 'sample_1_0_release.json'
        )

        store.store_file_from_local("test.json", "http://example.com", "release_package", "utf-8", json_filename)

        # Check Number of check results
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.record_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.record_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

        # Call Checks
        checks = Checks(self.database, collection)
        checks.process_all_files()

        # Check Number of check results
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.record_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_table])
            result = connection.execute(s)
            assert 1 == result.rowcount
            data = result.fetchone()
            assert not data.override_schema_version

            s = sa.sql.select([self.database.record_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

        # Call Checks Again - that should be fine
        checks = Checks(self.database, collection)
        checks.process_all_files()

        # Check Number of check results
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.record_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_table])
            result = connection.execute(s)
            assert 1 == result.rowcount
            data = result.fetchone()
            assert not data.override_schema_version

            s = sa.sql.select([self.database.record_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount


class TestCheckOlderThan11On(BaseDataBaseTest):

    def alter_config(self):
        self.config.default_value_collection_check_data = False
        self.config.default_value_collection_check_older_data_with_schema_version_1_1 = True

    def test_records(self):

        collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)
        collection = self.database.get_collection(collection_id)

        store = Store(self.config, self.database)
        store.set_collection(collection)

        json_filename = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'data', 'sample_1_0_record.json'
        )

        store.store_file_from_local("test.json", "http://example.com", "record", "utf-8", json_filename)

        # Check Number of check results
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.record_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.record_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

        # Call Checks
        checks = Checks(self.database, collection)
        checks.process_all_files()

        # Check Number of check results
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.record_check_table])
            result = connection.execute(s)
            assert 1 == result.rowcount
            data = result.fetchone()
            assert '1.1' == data.override_schema_version

            s = sa.sql.select([self.database.release_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.record_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

        # Call Checks Again - that should be fine
        checks = Checks(self.database, collection)
        checks.process_all_files()

        # Check Number of check results
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.record_check_table])
            result = connection.execute(s)
            assert 1 == result.rowcount
            data = result.fetchone()
            assert '1.1' == data.override_schema_version

            s = sa.sql.select([self.database.release_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.record_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

    def test_release(self):

        collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)
        collection = self.database.get_collection(collection_id)

        store = Store(self.config, self.database)
        store.set_collection(collection)

        json_filename = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'data', 'sample_1_0_release.json'
        )

        store.store_file_from_local("test.json", "http://example.com", "release_package", "utf-8", json_filename)

        # Check Number of check results
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.record_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.record_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

        # Call Checks
        checks = Checks(self.database, collection)
        checks.process_all_files()

        # Check Number of check results
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.record_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_table])
            result = connection.execute(s)
            assert 1 == result.rowcount
            data = result.fetchone()
            assert '1.1' == data.override_schema_version

            s = sa.sql.select([self.database.record_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

        # Call Checks Again - that should be fine
        checks = Checks(self.database, collection)
        checks.process_all_files()

        # Check Number of check results
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.record_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_table])
            result = connection.execute(s)
            assert 1 == result.rowcount
            data = result.fetchone()
            assert '1.1' == data.override_schema_version

            s = sa.sql.select([self.database.record_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount


class TestCheckAllOn(BaseDataBaseTest):

    def alter_config(self):
        self.config.default_value_collection_check_data = True
        self.config.default_value_collection_check_older_data_with_schema_version_1_1 = True

    def test_records(self):

        collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)
        collection = self.database.get_collection(collection_id)

        store = Store(self.config, self.database)
        store.set_collection(collection)

        json_filename = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'data', 'sample_1_0_record.json'
        )

        store.store_file_from_local("test.json", "http://example.com", "record", "utf-8", json_filename)

        # Check Number of check results
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.record_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.record_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

        # Call Checks
        checks = Checks(self.database, collection)
        checks.process_all_files()

        # Check Number of check results
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.record_check_table])
            result = connection.execute(s)
            assert 2 == result.rowcount

            s = sa.sql.select([self.database.release_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.record_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

        # Call Checks Again - that should be fine
        checks = Checks(self.database, collection)
        checks.process_all_files()

        # Check Number of check results
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.record_check_table])
            result = connection.execute(s)
            assert 2 == result.rowcount

            s = sa.sql.select([self.database.release_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.record_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

    def test_releases(self):

        self.config.default_value_collection_check_data = True
        self.config.default_value_collection_check_older_data_with_schema_version_1_1 = True

        collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)
        collection = self.database.get_collection(collection_id)

        store = Store(self.config, self.database)
        store.set_collection(collection)

        json_filename = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'data', 'sample_1_0_release.json'
        )

        store.store_file_from_local("test.json", "http://example.com", "release_package", "utf-8", json_filename)

        # Check Number of check results
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.record_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.record_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

        # Call Checks
        checks = Checks(self.database, collection)
        checks.process_all_files()

        # Check Number of check results
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.record_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_table])
            result = connection.execute(s)
            assert 2 == result.rowcount

            s = sa.sql.select([self.database.record_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

        # Call Checks Again - that should be fine
        checks = Checks(self.database, collection)
        checks.process_all_files()

        # Check Number of check results
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.record_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_table])
            result = connection.execute(s)
            assert 2 == result.rowcount

            s = sa.sql.select([self.database.record_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount


class TestCheck11NotSelected(BaseDataBaseTest):

    def alter_config(self):
        self.config.default_value_collection_check_data = False
        self.config.default_value_collection_check_older_data_with_schema_version_1_1 = True

    def test_records(self):

        collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)
        collection = self.database.get_collection(collection_id)

        store = Store(self.config, self.database)
        store.set_collection(collection)

        json_filename = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'data', 'sample_1_1_record.json'
        )

        store.store_file_from_local("test.json", "http://example.com", "record", "utf-8", json_filename)

        assert len([res for res in self.database.get_records_to_check(collection_id, override_schema_version='1.1')]) == 0
        assert len([res for res in self.database.get_records_to_check(collection_id, override_schema_version='1.2')]) == 1

    def test_releases(self):

        collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)
        collection = self.database.get_collection(collection_id)

        store = Store(self.config, self.database)
        store.set_collection(collection)

        json_filename = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'data', 'sample_1_1_releases_multiple_with_same_ocid.json'
        )

        store.store_file_from_local("test.json", "http://example.com", "release_package", "utf-8", json_filename)

        assert len([res for res in self.database.get_releases_to_check(collection_id, override_schema_version='1.1')]) == 0
        # the file has 6 releases
        assert len([res for res in self.database.get_releases_to_check(collection_id, override_schema_version='1.2')]) == 6
