import datetime
import os
import sqlalchemy as sa
from ocdskingfisherprocess.store import Store
from tests.base import BaseDataBaseTest


class TestDelete(BaseDataBaseTest):

    def test_a_single_collection(self):

        collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)
        collection = self.database.get_collection(collection_id)

        store = Store(self.config, self.database)
        store.set_collection(collection)

        json_filename = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'data', 'sample_1_0_record.json'
        )

        store.store_file_from_local("test.json", "http://example.com", "record", "utf-8", json_filename)

        # Check Number of rows in various tables
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.collection_table])
            result = connection.execute(s)
            assert 1 == result.rowcount

            s = sa.sql.select([self.database.collection_file_table])
            result = connection.execute(s)
            assert 1 == result.rowcount

            s = sa.sql.select([self.database.collection_file_item_table])
            result = connection.execute(s)
            assert 1 == result.rowcount

            s = sa.sql.select([self.database.record_table])
            result = connection.execute(s)
            assert 1 == result.rowcount

            s = sa.sql.select([self.database.data_table])
            result = connection.execute(s)
            assert 1 == result.rowcount

            s = sa.sql.select([self.database.package_data_table])
            result = connection.execute(s)
            assert 1 == result.rowcount

        # Delete
        self.database.mark_collection_deleted_at(collection_id)
        self.database.delete_collection(collection_id)

        # Check Number of rows in various tables
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.collection_table])
            result = connection.execute(s)
            assert 1 == result.rowcount

            s = sa.sql.select([self.database.collection_file_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.collection_file_item_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.record_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.data_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

            s = sa.sql.select([self.database.package_data_table])
            result = connection.execute(s)
            assert 0 == result.rowcount
