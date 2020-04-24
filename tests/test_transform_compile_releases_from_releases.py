import datetime
import os

import sqlalchemy as sa

from ocdskingfisherprocess.store import Store
from ocdskingfisherprocess.transform import TRANSFORM_TYPE_COMPILE_RELEASES
from ocdskingfisherprocess.transform.compile_releases import CompileReleasesTransform
from tests.base import BaseDataBaseTest


class TestTransformCompileReleasesFromReleases(BaseDataBaseTest):

    def _setup_collections_and_data_run_transform(self, filename):

        # Make source collection
        source_collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)
        source_collection = self.database.get_collection(source_collection_id)

        # Load some data
        store = Store(self.config, self.database)
        store.set_collection(source_collection)
        json_filename = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'fixtures', filename
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

        return source_collection_id, source_collection, destination_collection_id, destination_collection

    def test_1(self):

        source_collection_id, source_collection, destination_collection_id, destination_collection = \
            self._setup_collections_and_data_run_transform('sample_1_1_releases_multiple_with_same_ocid.json')

        # check
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.compiled_release_table])
            result = connection.execute(s)
            assert 1 == result.rowcount

            # Check a couple of fields just to sanity check it's a compiled release
            compiled_release = result.fetchone()
            data = self.database.get_data(compiled_release['data_id'])
            assert 'ocds-213czf-000-00001-2011-01-10T09:30:00Z' == data.get('id')
            assert '2011-01-10T09:30:00Z' == data.get('date')
            assert 'ocds-213czf-000-00001' == data.get('ocid')

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

    def test_one_compiled(self):

        source_collection_id, source_collection, destination_collection_id, destination_collection = \
            self._setup_collections_and_data_run_transform('sample_1_1_releases_one_compiled.json')

        # Check warning
        files = self.database.get_all_files_in_collection(destination_collection_id)
        assert len(files) == 1
        file_items = self.database.get_all_files_items_in_file(files[0])
        assert len(file_items) == 1
        assert len(file_items[0].warnings) == 1
        assert 'This already has one compiled release in the source! ' + \
               'We have passed it through this transform unchanged.' \
               == file_items[0].warnings[0]

    def test_two_compiled_with_same_ocid(self):

        source_collection_id, source_collection, destination_collection_id, destination_collection = \
            self._setup_collections_and_data_run_transform('sample_1_1_releases_two_compiled_with_same_ocid.json')

        # Check warning
        files = self.database.get_all_files_in_collection(destination_collection_id)
        assert len(files) == 1
        file_items = self.database.get_all_files_items_in_file(files[0])
        assert len(file_items) == 1
        assert len(file_items[0].warnings) == 1
        assert 'This already has multiple compiled releases in the source! We have picked one at random and passed it through this transform unchanged.' == file_items[0].warnings[0]  # noqa

    def test_no_dates(self):

        source_collection_id, source_collection, destination_collection_id, destination_collection = \
            self._setup_collections_and_data_run_transform('sample_1_0_releases_no_dates.json')

        # Check collection notes
        notes = self.database.get_all_notes_in_collection(destination_collection_id)
        assert len(notes) == 1
        assert 'OCID ocds-213czf-000-00001 could not be compiled because ' +\
               'there are no releases with dates nor a release with a tag of "compiled".' == \
               notes[0].note

    def test_some_dates(self):

        source_collection_id, source_collection, destination_collection_id, destination_collection = \
            self._setup_collections_and_data_run_transform('sample_1_0_releases_some_dates.json')

        # check
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.compiled_release_table])
            result = connection.execute(s)
            assert 1 == result.rowcount

            # Check a couple of fields just to sanity check it's a compiled release
            compiled_release = result.fetchone()
            data = self.database.get_data(compiled_release['data_id'])
            assert 'ocds-213czf-000-00001-2010-03-15T09:30:00Z' == data.get('id')
            assert '2010-03-15T09:30:00Z' == data.get('date')
            assert 'ocds-213czf-000-00001' == data.get('ocid')

        # Check warning
        files = self.database.get_all_files_in_collection(destination_collection_id)
        assert len(files) == 1
        file_items = self.database.get_all_files_items_in_file(files[0])
        assert len(file_items) == 1
        assert len(file_items[0].warnings) == 1
        assert 'This OCID had some releases without a date element. We have compiled all other releases.' == file_items[0].warnings[0]  # noqa

        # Check collection notes
        notes = self.database.get_all_notes_in_collection(destination_collection_id)
        assert len(notes) == 0
