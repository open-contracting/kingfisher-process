from tests.base import BaseDataBaseTest
import datetime
import sqlalchemy as sa
from ocdskingfisherprocess.store import Store
from ocdskingfisherprocess.transform.compile_releases import CompileReleasesTransform
from ocdskingfisherprocess.transform import TRANSFORM_TYPE_COMPILE_RELEASES

import os


class TestTransformCompileReleasesUseExisting(BaseDataBaseTest):

    def test_1_from_records(self):
        # Make source collection
        source_collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)
        source_collection = self.database.get_collection(source_collection_id)

        # Load some data
        store = Store(self.config, self.database)
        store.set_collection(source_collection)
        json_filename = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'data', 'sample_1_1_record_with_dodgy_compiled_release.json'
        )
        store.store_file_from_local("test.json", "http://example.com", "record_package", "utf-8", json_filename)

        # Make destination collection
        destination_collection_id = self.database.get_or_create_collection_id(
            source_collection.source_id,
            source_collection.data_version,
            source_collection.sample,
            transform_from_collection_id=source_collection_id,
            transform_type=TRANSFORM_TYPE_COMPILE_RELEASES,
            create_options={
                'transform-use-existing-compiled-releases': False
            }
        )
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

