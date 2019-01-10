import datetime
import os
import sqlalchemy as sa
from ocdskingfisherprocess.store import Store
from tests.base import BaseTest
from ocdskingfisherprocess.checks import Checks


class TestCheckRecord(BaseTest):

    def test_check_defaults_off(self):

        self.config.default_value_collection_check_data = False
        self.config.default_value_collection_check_older_data_with_schema_version_1_1 = False

        self.setup_main_database()

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

    def test_check_check_default_on(self):

        self.config.default_value_collection_check_data = True
        self.config.default_value_collection_check_older_data_with_schema_version_1_1 = False

        self.setup_main_database()

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

            s = sa.sql.select([self.database.release_check_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.record_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

    def test_check_check_default_on_check_older_as_1_1_on(self):

        self.config.default_value_collection_check_data = True
        self.config.default_value_collection_check_older_data_with_schema_version_1_1 = True

        self.setup_main_database()

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


class TestCheckRelease(BaseTest):

    def test_check_defaults_off(self):

        self.config.default_value_collection_check_data = False
        self.config.default_value_collection_check_older_data_with_schema_version_1_1 = False

        self.setup_main_database()

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

    def test_check_check_default_on(self):

        self.config.default_value_collection_check_data = True
        self.config.default_value_collection_check_older_data_with_schema_version_1_1 = False

        self.setup_main_database()

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

            s = sa.sql.select([self.database.record_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.release_check_error_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

    def test_check_check_default_on_check_older_as_1_1_on(self):

        self.config.default_value_collection_check_data = True
        self.config.default_value_collection_check_older_data_with_schema_version_1_1 = True

        self.setup_main_database()

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
