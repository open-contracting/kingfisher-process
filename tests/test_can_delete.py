import datetime
from ocdskingfisherprocess.transform import TRANSFORM_TYPE_UPGRADE_1_0_TO_1_1
from tests.base import BaseDataBaseTest


class TestDelete(BaseDataBaseTest):
    def alter_config(self):
        self.config.run_standard_pipeline = False

    def test_a_single_collection(self):

        collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)

        # We can delete this
        assert self.database.can_mark_collection_deleted(collection_id)

    def test_a_finished_transform(self):

        source_collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)

        destination_collection_id = self.database.get_or_create_collection_id(
            "test",
            datetime.datetime.now(),
            False,
            transform_from_collection_id=source_collection_id,
            transform_type=TRANSFORM_TYPE_UPGRADE_1_0_TO_1_1)

        # they are both finished - just do what would happen in that case
        self.database.mark_collection_store_done(source_collection_id)
        self.database.mark_collection_store_done(destination_collection_id)

        # We can delete that source
        assert self.database.can_mark_collection_deleted(source_collection_id)

        # We can delete that destination
        assert self.database.can_mark_collection_deleted(destination_collection_id)

    def test_a_deleted_transform(self):

        source_collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)

        destination_collection_id = self.database.get_or_create_collection_id(
            "test",
            datetime.datetime.now(),
            False,
            transform_from_collection_id=source_collection_id,
            transform_type=TRANSFORM_TYPE_UPGRADE_1_0_TO_1_1)

        self.database.mark_collection_deleted_at(destination_collection_id)

        # We can delete that source, as the destination is deleted
        assert self.database.can_mark_collection_deleted(source_collection_id)

    def test_an_active_transform(self):

        source_collection_id = self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)

        self.database.get_or_create_collection_id("test", datetime.datetime.now(), False,
                                                  transform_from_collection_id=source_collection_id,
                                                  transform_type=TRANSFORM_TYPE_UPGRADE_1_0_TO_1_1)

        # We can NOT delete that source, as the transform is still active
        assert not self.database.can_mark_collection_deleted(source_collection_id)
