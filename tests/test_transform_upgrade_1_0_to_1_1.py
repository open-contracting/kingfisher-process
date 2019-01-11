from tests.base import BaseTest
import datetime
import sqlalchemy as sa
from ocdskingfisherprocess.store import Store
from ocdskingfisherprocess.transform.upgrade_1_0_to_1_1 import Upgrade10To11Transform
import os


class TestTransformUpgrade10To11(BaseTest):

    def test_record_1(self):
        self.setup_main_database()

        # Make source collection
        source_collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)
        source_collection = self.database.get_collection(source_collection_id)

        # Load some data
        store = Store(self.config, self.database)
        store.set_collection(source_collection)
        json_filename = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'data', 'sample_1_0_record.json'
        )
        store.store_file_from_local("test.json", "http://example.com", "record_package", "utf-8", json_filename)

        # Make destination collection
        destination_collection_id = self.database.get_or_create_collection_id(
            source_collection.source_id,
            source_collection.data_version,
            source_collection.sample,
            transform_from_collection_id=source_collection_id,
            transform_type=Upgrade10To11Transform.type)
        destination_collection = self.database.get_collection(destination_collection_id)

        # transform!
        transform = Upgrade10To11Transform(self.config, self.database, destination_collection)
        transform.process()

        # check
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.record_table])
            result = connection.execute(s)
            assert 2 == result.rowcount

            s = sa.sql.select([self.database.release_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.transform_upgrade_1_0_to_1_1_status_record_table])
            result = connection.execute(s)
            assert 1 == result.rowcount

            s = sa.sql.select([self.database.transform_upgrade_1_0_to_1_1_status_release_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

        # transform again! This should be fine
        transform = Upgrade10To11Transform(self.config, self.database, destination_collection)
        transform.process()

        # check
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.record_table])
            result = connection.execute(s)
            assert 2 == result.rowcount

            s = sa.sql.select([self.database.release_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.transform_upgrade_1_0_to_1_1_status_record_table])
            result = connection.execute(s)
            assert 1 == result.rowcount

            s = sa.sql.select([self.database.transform_upgrade_1_0_to_1_1_status_release_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

    def test_release_1(self):
        self.setup_main_database()

        # Make source collection
        source_collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)
        source_collection = self.database.get_collection(source_collection_id)

        # Load some data
        store = Store(self.config, self.database)
        store.set_collection(source_collection)
        json_filename = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'data', 'sample_1_0_release.json'
        )
        store.store_file_from_local("test.json", "http://example.com", "release_package", "utf-8", json_filename)

        # Make destination collection
        destination_collection_id = self.database.get_or_create_collection_id(
            source_collection.source_id,
            source_collection.data_version,
            source_collection.sample,
            transform_from_collection_id=source_collection_id,
            transform_type=Upgrade10To11Transform.type)
        destination_collection = self.database.get_collection(destination_collection_id)

        # transform!
        transform = Upgrade10To11Transform(self.config, self.database, destination_collection)
        transform.process()

        # check
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.release_table])
            result = connection.execute(s)
            assert 2 == result.rowcount

            s = sa.sql.select([self.database.record_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.transform_upgrade_1_0_to_1_1_status_record_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.transform_upgrade_1_0_to_1_1_status_release_table])
            result = connection.execute(s)
            assert 1 == result.rowcount

        # transform again! This should be fine
        transform = Upgrade10To11Transform(self.config, self.database, destination_collection)
        transform.process()

        # check
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.release_table])
            result = connection.execute(s)
            assert 2 == result.rowcount

            s = sa.sql.select([self.database.record_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.transform_upgrade_1_0_to_1_1_status_record_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.transform_upgrade_1_0_to_1_1_status_release_table])
            result = connection.execute(s)
            assert 1 == result.rowcount
