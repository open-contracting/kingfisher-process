from tests.base import BaseDataBaseTest
import datetime
import sqlalchemy as sa
from ocdskingfisherprocess.store import Store
from ocdskingfisherprocess.transform.compile_releases import CompileReleasesTransform
from ocdskingfisherprocess.transform import TRANSFORM_TYPE_COMPILE_RELEASES

import os


class TestTransformCompileReleasesUseExisting(BaseDataBaseTest):

    def test_1_from_releases(self):
        # Make source collection
        source_collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)
        source_collection = self.database.get_collection(source_collection_id)

        # Load some data
        store = Store(self.config, self.database)
        store.set_collection(source_collection)
        json_filename = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'data', 'sample_1_1_releases_multiple_with_same_ocid.json'
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

        # check
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.compiled_release_table])
            result = connection.execute(s)
            assert 1 == result.rowcount

        # transform again! This should be fine
        transform = CompileReleasesTransform(self.config, self.database, destination_collection)
        transform.process()

        # check
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.compiled_release_table])
            result = connection.execute(s)
            assert 1 == result.rowcount

        # destination collection should be closed
        destination_collection = self.database.get_collection(destination_collection_id)
        assert destination_collection.store_end_at != None # noqa

    def test_1_from_records(self):
        # Make source collection
        source_collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)
        source_collection = self.database.get_collection(source_collection_id)

        # Load some data
        store = Store(self.config, self.database)
        store.set_collection(source_collection)
        json_filename = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'data', 'sample_1_1_record.json'
        )
        store.store_file_from_local("test.json", "http://example.com", "record_package", "utf-8", json_filename)

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

        # check
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.compiled_release_table])
            result = connection.execute(s)
            assert 1 == result.rowcount
            compiled_release = result.fetchone()

        # We should be in "Use existing compiled release" mode. Check something from the source file.
        compiled_release_data = self.database.get_data(compiled_release['data_id'])
        assert compiled_release_data['buyer']['name'] == 'London Borough of Barnet'

        # transform again! This should be fine
        transform = CompileReleasesTransform(self.config, self.database, destination_collection)
        transform.process()

        # check
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.compiled_release_table])
            result = connection.execute(s)
            assert 1 == result.rowcount

        # destination collection should be closed
        destination_collection = self.database.get_collection(destination_collection_id)
        assert destination_collection.store_end_at != None # noqa

    def test_one_compiled_from_releases(self):

        # Make source collection
        source_collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)
        source_collection = self.database.get_collection(source_collection_id)

        # Load some data
        store = Store(self.config, self.database)
        store.set_collection(source_collection)
        json_filename = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'data', 'sample_1_1_releases_one_compiled.json'
        )
        store.store_file_from_local("test.json", "http://example.com", "release_package", "utf-8", json_filename)

        # Mark source collection as finished
        self.database.mark_collection_store_done(source_collection_id)

        # Make destination collection
        destination_collection_id = self.database.get_or_create_collection_id(
            source_collection.source_id,
            source_collection.data_version,
            source_collection.sample,
            transform_from_collection_id=source_collection_id,
            transform_type=TRANSFORM_TYPE_COMPILE_RELEASES)
        destination_collection = self.database.get_collection(destination_collection_id)

        # transform!
        transform = CompileReleasesTransform(self.config, self.database, destination_collection)
        transform.process()

        # Check warning
        files = self.database.get_all_files_in_collection(destination_collection_id)
        assert len(files) == 1
        file_items = self.database.get_all_files_items_in_file(files[0])
        assert len(file_items) == 1
        assert len(file_items[0].warnings) == 1
        assert 'This already has one compiled release in the source! ' + \
               'We have passed it through this transform unchanged.' \
               == file_items[0].warnings[0]

    def test_two_compiled_with_same_ocid_from_releases(self):

        # Make source collection
        source_collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)
        source_collection = self.database.get_collection(source_collection_id)

        # Load some data
        store = Store(self.config, self.database)
        store.set_collection(source_collection)
        json_filename = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'data', 'sample_1_1_releases_two_compiled_with_same_ocid.json'
        )
        store.store_file_from_local("test.json", "http://example.com", "release_package", "utf-8", json_filename)

        # Mark source collection as finished
        self.database.mark_collection_store_done(source_collection_id)

        # Make destination collection
        destination_collection_id = self.database.get_or_create_collection_id(
            source_collection.source_id,
            source_collection.data_version,
            source_collection.sample,
            transform_from_collection_id=source_collection_id,
            transform_type=TRANSFORM_TYPE_COMPILE_RELEASES)
        destination_collection = self.database.get_collection(destination_collection_id)

        # transform!
        transform = CompileReleasesTransform(self.config, self.database, destination_collection)
        transform.process()

        # Check warning
        files = self.database.get_all_files_in_collection(destination_collection_id)
        assert len(files) == 1
        file_items = self.database.get_all_files_items_in_file(files[0])
        assert len(file_items) == 1
        assert len(file_items[0].warnings) == 1
        assert 'This already has multiple compiled releases in the source! We have picked one at random and passed it through this transform unchanged.' == file_items[0].warnings[0]  # noqa
