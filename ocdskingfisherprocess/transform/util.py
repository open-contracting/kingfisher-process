from ocdskingfisherprocess.transform.compile_releases import CompileReleasesTransform
from ocdskingfisherprocess.transform.upgrade_1_0_to_1_1 import Upgrade10To11Transform


def get_transform_instance(type, config, database, destination_collection):
    if type == CompileReleasesTransform.type:
        return CompileReleasesTransform(config, database, destination_collection)
    elif type == Upgrade10To11Transform.type:
        return Upgrade10To11Transform(config, database, destination_collection)
    else:
        raise Exception("That transform type is not known")
