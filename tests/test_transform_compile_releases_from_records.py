import datetime
import os

import sqlalchemy as sa

from ocdskingfisherprocess.store import Store
from ocdskingfisherprocess.transform import TRANSFORM_TYPE_COMPILE_RELEASES
from ocdskingfisherprocess.transform.compile_releases import CompileReleasesTransform
from tests.base import BaseDataBaseTest


class TestTransformCompileReleasesFromRecords(BaseDataBaseTest):

    def _setup(self, filename):

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
            self._setup('sample_1_1_record.json')

        # check
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.compiled_release_table])
            result = connection.execute(s)
            assert 1 == result.rowcount

            # Check a couple of fields just to sanity check it's the compiled release in the record table
            complied_release = result.fetchone()
            data = self.database.get_data(complied_release['data_id'])
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

    def test_no_compiled_release_linked_records_so_cant_do_anything(self):

        source_collection_id, source_collection, destination_collection_id, destination_collection = \
            self._setup('sample_1_1_record_linked_releases_not_compiled.json')

        # check
        with self.database.get_engine().begin() as connection:
            s = sa.sql.select([self.database.compiled_release_table])
            result = connection.execute(s)
            assert 0 == result.rowcount
