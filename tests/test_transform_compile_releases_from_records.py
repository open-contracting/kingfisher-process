import datetime
import os

import sqlalchemy as sa

from ocdskingfisherprocess.store import Store
from ocdskingfisherprocess.transform import TRANSFORM_TYPE_COMPILE_RELEASES
from ocdskingfisherprocess.transform.compile_releases import CompileReleasesTransform
from tests.base import BaseDataBaseTest


class TestTransformCompileReleasesFromRecords(BaseDataBaseTest):

    def _setup_collections_and_data_run_transform(self, filename, load_a_second_time=False):

        # Make source collection
        source_collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)
        source_collection = self.database.get_collection(source_collection_id)

        # Load some data
        store = Store(self.config, self.database)
        store.set_collection(source_collection)
        json_filename = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'fixtures', filename
        )
        store.store_file_from_local("test.json", "http://example.com", "record_package", "utf-8", json_filename)
        if load_a_second_time:
            store.store_file_from_local("test2.json", "http://example.com", "record_package", "utf-8", json_filename)

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

    def test_compiled_release(self):

        source_collection_id, source_collection, destination_collection_id, destination_collection = \
            self._setup_collections_and_data_run_transform('sample_1_1_record.json')

        # check
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.compiled_release_table])
            result = connection.execute(s)
            assert 1 == result.rowcount

            # Check a couple of fields just to sanity check it's the compiled release in the record table
            # Because releases are linked, the only way to get data is to take the compiled release
            compiled_release = result.fetchone()
            data = self.database.get_data(compiled_release['data_id'])
            assert 'ocds-213czf-000-00001-2011-01-10T09:30:00Z' == data.get('id')
            assert '2011-01-10T09:30:00Z' == data.get('date')
            assert 'ocds-213czf-000-00001' == data.get('ocid')

            # Check warnings
            s = sa.sql.select([self.database.collection_file_item_table])\
                .where(self.database.collection_file_item_table.c.id == compiled_release['collection_file_item_id'])
            result_file_item = connection.execute(s)
            assert 1 == result_file_item.rowcount
            collection_file_item = result_file_item.fetchone()
            assert collection_file_item.warnings == ['This already had a compiledRelease in the record! It was passed through this transform unchanged.']  # noqa

        # Check collection notes
        notes = self.database.get_all_notes_in_collection(destination_collection_id)
        assert len(notes) == 0

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

    def test_no_compiled_release_linked_records_so_cant_do_anything(self):

        source_collection_id, source_collection, destination_collection_id, destination_collection = \
            self._setup_collections_and_data_run_transform('sample_1_1_record_linked_releases_not_compiled.json')

        # check
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.compiled_release_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

        # Check collection notes
        notes = self.database.get_all_notes_in_collection(destination_collection_id)
        assert len(notes) == 1
        assert 'OCID ocds-213czf-000-00001 could not be compiled because at least one release in the releases ' +\
               'array is a linked release or there are no releases with dates, ' +\
               'and the record has neither a compileRelease nor a release with a tag of "compiled".' == \
               notes[0].note

    def test_transform_compiles(self):
        """This data files has full releases and nothing else, so the transform should compile itself using ocdsmerge"""  # noqa

        source_collection_id, source_collection, destination_collection_id, destination_collection = \
            self._setup_collections_and_data_run_transform('sample_1_1_record_releases_not_compiled.json')

        # check
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.compiled_release_table])
            result = connection.execute(s)
            assert 1 == result.rowcount

            # Check a couple of fields just to sanity check we've compiled something here.
            # The only way it could get data here is if it compiled it itself.
            compiled_release = result.fetchone()
            data = self.database.get_data(compiled_release['data_id'])
            assert 'ocds-213czf-000-00001-2011-01-10T09:30:00Z' == data.get('id')
            assert '2011-01-10T09:30:00Z' == data.get('date')
            assert 'ocds-213czf-000-00001' == data.get('ocid')

            # Check warnings
            s = sa.sql.select([self.database.collection_file_item_table])\
                .where(self.database.collection_file_item_table.c.id == compiled_release['collection_file_item_id'])
            result_file_item = connection.execute(s)
            assert 1 == result_file_item.rowcount
            collection_file_item = result_file_item.fetchone()
            assert collection_file_item.warnings == None  # noqa

        # Check collection notes
        notes = self.database.get_all_notes_in_collection(destination_collection_id)
        assert len(notes) == 0

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

    def test_two_records_same_ocid(self):
        source_collection_id, source_collection, destination_collection_id, destination_collection = \
            self._setup_collections_and_data_run_transform(
                'sample_1_1_record_releases_not_compiled.json',
                load_a_second_time=True
            )

        # check
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.compiled_release_table])
            result = connection.execute(s)
            assert 1 == result.rowcount

            # Check a couple of fields just to sanity check it's the compiled release in the record table
            # Because releases are linked, the only way to get data is to take the compiled release
            compiled_release = result.fetchone()
            data = self.database.get_data(compiled_release['data_id'])
            assert 'ocds-213czf-000-00001-2011-01-10T09:30:00Z' == data.get('id')
            assert '2011-01-10T09:30:00Z' == data.get('date')
            assert 'ocds-213czf-000-00001' == data.get('ocid')

            # Check warnings
            s = sa.sql.select([self.database.collection_file_item_table]) \
                .where(self.database.collection_file_item_table.c.id == compiled_release['collection_file_item_id'])
            result_file_item = connection.execute(s)
            assert 1 == result_file_item.rowcount
            collection_file_item = result_file_item.fetchone()
            assert collection_file_item.warnings == ['There are multiple records for this OCID! The record to pass through was selected arbitrarily.']  # noqa

        # Check collection notes
        notes = self.database.get_all_notes_in_collection(destination_collection_id)
        assert len(notes) == 0

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
        assert destination_collection.store_end_at != None  # noqa

    def test_no_dates(self):

        source_collection_id, source_collection, destination_collection_id, destination_collection = \
            self._setup_collections_and_data_run_transform('sample_1_1_record_releases_not_compiled_no_dates.json')

        # check
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.compiled_release_table])
            result = connection.execute(s)
            assert 0 == result.rowcount

        # Check collection notes
        notes = self.database.get_all_notes_in_collection(destination_collection_id)
        assert len(notes) == 1
        assert 'OCID ocds-213czf-000-00001 could not be compiled ' +\
               'because at least one release in the releases array is a ' +\
               'linked release or there are no releases with dates, ' +\
               'and the record has neither a compileRelease nor a release with a tag of "compiled".' == \
               notes[0].note

    def test_some_dates(self):

        source_collection_id, source_collection, destination_collection_id, destination_collection = \
            self._setup_collections_and_data_run_transform('sample_1_1_record_releases_not_compiled_some_dates.json')

        # check
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.compiled_release_table])
            result = connection.execute(s)
            assert 1 == result.rowcount

            # Check a couple of fields just to sanity check it's the compiled release in the record table
            # Because releases are linked, the only way to get data is to take the compiled release
            compiled_release = result.fetchone()
            data = self.database.get_data(compiled_release['data_id'])
            assert 'ocds-213czf-000-00001-2011-01-10T09:30:00Z' == data.get('id')
            assert '2011-01-10T09:30:00Z' == data.get('date')
            assert 'ocds-213czf-000-00001' == data.get('ocid')

            # Check warnings
            s = sa.sql.select([self.database.collection_file_item_table]) \
                .where(self.database.collection_file_item_table.c.id == compiled_release['collection_file_item_id'])
            result_file_item = connection.execute(s)
            assert 1 == result_file_item.rowcount
            collection_file_item = result_file_item.fetchone()
            assert collection_file_item.warnings == [
                'This OCID had some releases without a date element. We have compiled all other releases.']

        # Check collection notes
        notes = self.database.get_all_notes_in_collection(destination_collection_id)
        assert len(notes) == 0
