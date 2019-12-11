from ocdskingfisherprocess.transform import TRANSFORM_TYPE_COMPILE_RELEASES
from ocdskingfisherprocess.transform.compile_releases import CompileReleasesTransform
from tests.base import BaseDataBaseTest


class TestTransformCompileReleases(BaseDataBaseTest):
    def test_1(self):
        source_collection_id, source_collection = self.get_collection_and_store_file(
            'sample_1_1_releases_multiple_with_same_ocid.json', 'release_package')
        destination_collection_id, destination_collection = self.get_transformed_collection(
            source_collection_id, source_collection, TRANSFORM_TYPE_COMPILE_RELEASES)

        # transform! Nothing should happen because source is not finished
        transform = CompileReleasesTransform(self.config, self.database, destination_collection)
        transform.process()
        with self.database.get_engine().begin() as connection:
            self.assert_row_count(connection, 'compiled_release', 0)

        self.database.mark_collection_store_done(source_collection_id)

        # transform! This should do the work.
        transform = CompileReleasesTransform(self.config, self.database, destination_collection)
        transform.process()
        with self.database.get_engine().begin() as connection:
            self.assert_row_count(connection, 'compiled_release', 1)

        # transform again! This should be fine
        transform = CompileReleasesTransform(self.config, self.database, destination_collection)
        transform.process()
        with self.database.get_engine().begin() as connection:
            self.assert_row_count(connection, 'compiled_release', 1)

        # destination collection should be closed
        destination_collection = self.database.get_collection(destination_collection_id)
        assert destination_collection.store_end_at is not None

    def test_one_compiled(self):
        source_collection_id, source_collection = self.get_collection_and_store_file(
            'sample_1_1_releases_one_compiled.json', 'release_package')

        self.database.mark_collection_store_done(source_collection_id)

        destination_collection_id, destination_collection = self.get_transformed_collection(
            source_collection_id, source_collection, TRANSFORM_TYPE_COMPILE_RELEASES)

        transform = CompileReleasesTransform(self.config, self.database, destination_collection)
        transform.process()

        files = self.database.get_all_files_in_collection(destination_collection_id)
        assert len(files) == 1
        file_items = self.database.get_all_files_items_in_file(files[0])
        assert len(file_items) == 1
        assert len(file_items[0].warnings) == 1
        assert 'This already has one compiled release in the source! We have passed it through this transform ' \
               'unchanged.' == file_items[0].warnings[0]

    def test_two_compiled_with_same_ocid(self):
        source_collection_id, source_collection = self.get_collection_and_store_file(
            'sample_1_1_releases_two_compiled_with_same_ocid.json', 'release_package')

        self.database.mark_collection_store_done(source_collection_id)

        destination_collection_id, destination_collection = self.get_transformed_collection(
            source_collection_id, source_collection, TRANSFORM_TYPE_COMPILE_RELEASES)

        transform = CompileReleasesTransform(self.config, self.database, destination_collection)
        transform.process()

        files = self.database.get_all_files_in_collection(destination_collection_id)
        assert len(files) == 1
        file_items = self.database.get_all_files_items_in_file(files[0])
        assert len(file_items) == 1
        assert len(file_items[0].warnings) == 1
        assert 'This already has multiple compiled releases in the source! We have picked one at random and passed ' \
               'it through this transform unchanged.' == file_items[0].warnings[0]
