import sqlalchemy as sa

from ocdskingfisherprocess.transform import TRANSFORM_TYPE_COMPILE_RELEASES
from ocdskingfisherprocess.transform.compile_releases import CompileReleasesTransform
from tests.base import BaseDataBaseTest


class TestCollectionCachedColums(BaseDataBaseTest):
    def alter_config(self):
        self.config.run_standard_pipeline = False

    def test_records(self):
        self.run('sample_1_0_record.json', 'record_package', 'cached_records_count', 1)

    def test_releases(self):
        self.run('sample_1_1_releases_multiple_with_same_ocid.json', 'release_package', 'cached_releases_count', 6)

    def run(self, filename, data_type, column, count):
        collection_id, collection = self.get_collection_and_store_file(filename, data_type)

        self.database.update_collection_cached_columns(collection_id)

        with self.database.get_engine().begin() as connection:
            result = self.assert_row_count(connection, 'collection', 1)
            data = result.fetchone()
            assert data[column] == count

    def test_compiled_releases(self):
        source_collection_id, source_collection = self.get_collection_and_store_file(
            'sample_1_1_releases_multiple_with_same_ocid.json', 'release_package')

        destination_collection_id, destination_collection = self.get_transformed_collection(
            source_collection_id, source_collection, TRANSFORM_TYPE_COMPILE_RELEASES)

        # transform! Nothing should happen because source is not finished
        transform = CompileReleasesTransform(self.config, self.database, destination_collection)
        transform.process()

        with self.database.get_engine().begin() as connection:
            result = self.assert_row_count(connection, 'compiled_release', 0)

        # Mark source collection as finished
        self.database.mark_collection_store_done(source_collection_id)

        # transform! This should do the work.
        transform = CompileReleasesTransform(self.config, self.database, destination_collection)
        transform.process()

        # update_collection_cached_columns
        self.database.update_collection_cached_columns(source_collection_id)
        self.database.update_collection_cached_columns(destination_collection_id)

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
