import datetime
import os
import sqlalchemy as sa
from ocdskingfisherprocess.store import Store
from ocdskingfisherprocess.transform import TRANSFORM_TYPE_UPGRADE_1_0_TO_1_1
from ocdskingfisherprocess.transform.upgrade_1_0_to_1_1 import Upgrade10To11Transform
from tests.base import BaseDataBaseTest


class TestDelete(BaseDataBaseTest):
    def alter_config(self):
        self.config.run_standard_pipeline = False

    def test_a_single_collection(self):
        collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)
        collection = self.database.get_collection(collection_id)

        store = Store(self.config, self.database)
        store.set_collection(collection)

        # Load several records to test data delete
        json_filename1 = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'data', 'sample_1_0_record.json'
        )
        store.store_file_from_local("test1.json", "http://example.com", "record", "utf-8", json_filename1)

        json_filename2 = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'data', 'sample_1_1_record.json'
        )
        store.store_file_from_local("test2.json", "http://example.com", "record", "utf-8", json_filename2)

        # Check Number of rows in various tables
        with self.database.get_engine().begin() as connection:
            self.assert_row_counts(connection, {
                'collection': 1,
                'collection_file': 2,
                'collection_file_item': 2,
                'record': 2,
                'data': 2,
                'package_data': 2,
            })

        # Delete
        self.database.mark_collection_deleted_at(collection_id)
        self.database.delete_collection(collection_id)
        self.database.delete_orphan_data()

        # Check Number of rows in various tables
        with self.database.get_engine().begin() as connection:
            self.assert_row_counts(connection, {
                'collection': 1,
                'collection_file': 0,
                'collection_file_item': 0,
                'record': 0,
                'data': 0,
                'package_data': 0,
            })

    def test_two_collections_with_upgrade_1_0_to_1_1(self):
        source_collection_id, source_collection = self.get_collection_and_store_file(
            'sample_1_0_record.json', 'record')

        self.database.mark_collection_store_done(source_collection_id)

        destination_collection_id, destination_collection = self.get_transformed_collection(
            source_collection_id, source_collection, TRANSFORM_TYPE_UPGRADE_1_0_TO_1_1)

        transform = Upgrade10To11Transform(self.config, self.database, destination_collection)
        transform.process()

        # Check Number of rows in various tables
        with self.database.get_engine().begin() as connection:
            self.assert_row_counts(connection, {
                'collection': 2,
                'collection_file': 2,
                'collection_file_item': 2,
                'record': 2,
                'transform_upgrade_1_0_to_1_1_status_record': 1,
            })

            s = sa.sql.select([self.database.data_table])
            result = connection.execute(s)
            assert 0 < result.rowcount

            s = sa.sql.select([self.database.package_data_table])
            result = connection.execute(s)
            assert 0 < result.rowcount

        # Delete
        self.database.mark_collection_deleted_at(source_collection_id)
        self.database.delete_collection(source_collection_id)
        self.database.delete_orphan_data()

        self.database.mark_collection_deleted_at(destination_collection_id)
        self.database.delete_collection(destination_collection_id)
        self.database.delete_orphan_data()

        # Check Number of rows in various tables
        with self.database.get_engine().begin() as connection:
            self.assert_row_counts(connection, {
                'collection': 2,
                'collection_file': 0,
                'collection_file_item': 0,
                'record': 0,
                'transform_upgrade_1_0_to_1_1_status_record': 0,
                'data': 0,
                'package_data': 0,
            })
