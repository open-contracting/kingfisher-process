import datetime
import os

import sqlalchemy as sa

from ocdskingfisherprocess.store import Store
from ocdskingfisherprocess.transform import TRANSFORM_TYPE_COMPILE_RELEASES
from ocdskingfisherprocess.transform.compile_releases import CompileReleasesTransform
from tests.base import BaseDataBaseTest


class TestCollectionCachedColums(BaseDataBaseTest):

    def alter_config(self):
        self.config.run_standard_pipeline = False

    def test_releases(self):
        # Make collection
        collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)
        collection = self.database.get_collection(collection_id)

        # Load some data
        store = Store(self.config, self.database)
        store.set_collection(collection)
        json_filename = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'fixtures', 'sample_1_1_releases_multiple_with_same_ocid.json'
        )
        store.store_file_from_local("test.json", "http://example.com", "release_package", "utf-8", json_filename)

        # test
        self.database.update_collection_cached_columns(collection_id)

        # check
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.collection_table])
            result = connection.execute(s)
            assert 1 == result.rowcount

            data = result.fetchone()
            assert 6 == data['cached_releases_count']

    def test_records(self):
        # Make collection
        collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)
        collection = self.database.get_collection(collection_id)

        # Load some data
        store = Store(self.config, self.database)
        store.set_collection(collection)
        json_filename = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'fixtures', 'sample_1_0_record.json'
        )
        store.store_file_from_local("test.json", "http://example.com", "record_package", "utf-8", json_filename)

        # test
        self.database.update_collection_cached_columns(collection_id)

        # check
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.collection_table])
            result = connection.execute(s)
            assert 1 == result.rowcount
            data = result.fetchone()
            assert 1 == data['cached_records_count']

    def test_compiled_releases(self):
        # Make source collection
        source_collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)
        source_collection = self.database.get_collection(source_collection_id)

        # Load some data
        store = Store(self.config, self.database)
        store.set_collection(source_collection)
        json_filename = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'fixtures', 'sample_1_1_releases_multiple_with_same_ocid.json'
        )
        store.store_file_from_local("test.json", "http://example.com", "release_package", "utf-8", json_filename)

        # Make destination collection
        destination_collection_id = self.database.get_or_create_collection_id(
            source_collection.source_id,
            source_collection.data_version,
            source_collection.sample,
            transform_from_collection_id=source_collection_id,
            transform_type=TRANSFORM_TYPE_COMPILE_RELEASES)
        destination_collection = self.database.get_collection(destination_collection_id)

        # transform! Nothing should happen because source is not finished
        transform = CompileReleasesTransform(self.config, self.database, destination_collection)
        transform.process()

        # check
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.compiled_release_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

        # Mark source collection as finished
        self.database.mark_collection_store_done(source_collection_id)

        # transform! This should do the work.
        transform = CompileReleasesTransform(self.config, self.database, destination_collection)
        transform.process()

        # update_collection_cached_columns
        self.database.update_collection_cached_columns(source_collection_id)
        self.database.update_collection_cached_columns(destination_collection_id)

        # check
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.collection_table]).order_by(self.database.collection_table.columns.id)
            result = connection.execute(s)
            assert 2 == result.rowcount
            data = result.fetchone()
            assert 6 == data['cached_releases_count']
            assert 0 == data['cached_compiled_releases_count']
            data = result.fetchone()
            assert 0 == data['cached_releases_count']
            assert 1 == data['cached_compiled_releases_count']
