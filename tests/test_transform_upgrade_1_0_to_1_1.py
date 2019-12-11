from ocdskingfisherprocess.transform import TRANSFORM_TYPE_UPGRADE_1_0_TO_1_1
from ocdskingfisherprocess.transform.upgrade_1_0_to_1_1 import Upgrade10To11Transform
from tests.base import BaseDataBaseTest


class TestTransformUpgrade10To11(BaseDataBaseTest):
    def test_record(self):
        self.run('sample_1_0_record.json', 'record_package', {
            'record': 2,
            'transform_upgrade_1_0_to_1_1_status_record': 1,
        })

    def test_release(self):
        self.run('sample_1_0_releases.json', 'release_package', {
                'release': 4,
                'transform_upgrade_1_0_to_1_1_status_release': 2,
        })

    def run(self, filename, data_type, counts):
        _counts = {
            'record': 0,
            'release': 0,
            'transform_upgrade_1_0_to_1_1_status_record': 0,
            'transform_upgrade_1_0_to_1_1_status_release': 0,
        }
        _counts.update(counts)

        source_collection_id, source_collection = self.get_collection_and_store_file(filename, data_type)
        destination_collection_id, destination_collection = self.get_transformed_collection(
            source_collection_id, source_collection, TRANSFORM_TYPE_UPGRADE_1_0_TO_1_1)

        self.transform(destination_collection, _counts)
        # Running another time should have no effect.
        self.transform(destination_collection, _counts)

        # The destination collection shouldn't be closed, because the source is still open.
        destination_collection = self.database.get_collection(destination_collection_id)
        assert destination_collection.store_end_at is None

        # Mark source collection as finished.
        self.database.mark_collection_store_done(source_collection_id)

        # Running another time should have no effect.
        self.transform(destination_collection, _counts)

        # The destination collection should be closed.
        destination_collection = self.database.get_collection(destination_collection_id)
        assert destination_collection.store_end_at is not None

    def transform(self, collection, counts):
        transform = Upgrade10To11Transform(self.config, self.database, collection)
        transform.process()

        with self.database.get_engine().begin() as connection:
            self.assert_row_counts(connection, counts)
