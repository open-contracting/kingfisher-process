from ocdskingfisherprocess.transform.compile_releases import CompileReleasesTransform
from ocdskingfisherprocess.transform.upgrade_1_0_to_1_1 import Upgrade10To11Transform
from ocdskingfisherprocess.transform import TRANSFORM_TYPE_COMPILE_RELEASES, TRANSFORM_TYPE_UPGRADE_1_0_TO_1_1


def get_transform_instance(type, config, database, destination_collection):
    if type == TRANSFORM_TYPE_COMPILE_RELEASES:
        return CompileReleasesTransform(config, database, destination_collection)
    elif type == TRANSFORM_TYPE_UPGRADE_1_0_TO_1_1:
        return Upgrade10To11Transform(config, database, destination_collection)
    else:
        raise Exception("That transform type is not known")
