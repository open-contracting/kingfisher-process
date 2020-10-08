import datetime

from ocdskingfisherprocess.transform import TRANSFORM_TYPE_COMPILE_RELEASES, TRANSFORM_TYPE_UPGRADE_1_0_TO_1_1
from tests.base import BaseDataBaseTest


class TestStandardPipelineOn(BaseDataBaseTest):

    def alter_config(self):
        self.config.run_standard_pipeline = True

    def test_check_on(self):

        self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)

        collections = self.database.get_all_collections()
        assert 3 == len(collections)

        assert collections[0].transform_type == ''
        assert collections[0].transform_from_collection_id is None

        assert TRANSFORM_TYPE_UPGRADE_1_0_TO_1_1 == collections[1].transform_type
        assert collections[0].database_id == collections[1].transform_from_collection_id

        assert TRANSFORM_TYPE_COMPILE_RELEASES == collections[2].transform_type
        assert collections[1].database_id == collections[2].transform_from_collection_id


class TestStandardPipelineOff(BaseDataBaseTest):

    def alter_config(self):
        self.config.run_standard_pipeline = False

    def test_check_off(self):

        self.database.get_or_create_collection_id("test", datetime.datetime.now(), False)

        collections = self.database.get_all_collections()
        assert 1 == len(collections)
