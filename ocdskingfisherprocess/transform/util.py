from ocdskingfisherprocess.transform import TRANSFORM_TYPE_COMPILE_RELEASES, TRANSFORM_TYPE_UPGRADE_1_0_TO_1_1
from ocdskingfisherprocess.transform.compile_releases import CompileReleasesTransform
from ocdskingfisherprocess.transform.upgrade_1_0_to_1_1 import Upgrade10To11Transform


def get_transform_instance(type, config, database, destination_collection, run_until_timestamp=None):
    if type == TRANSFORM_TYPE_COMPILE_RELEASES:
        return CompileReleasesTransform(
            config,
            database,
            destination_collection,
            run_until_timestamp=run_until_timestamp
        )
    elif type == TRANSFORM_TYPE_UPGRADE_1_0_TO_1_1:
        return Upgrade10To11Transform(
            config,
            database,
            destination_collection,
            run_until_timestamp=run_until_timestamp
        )
    else:
        raise Exception("That transform type is not known")
