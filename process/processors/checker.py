import logging

from django.db.utils import IntegrityError
from libcoveocds.api import ocds_json_output

from process.exceptions import AlreadyExists
from process.models import Collection, CollectionFile, Record, RecordCheck, Release, ReleaseCheck

# Get an instance of a logger
logger = logging.getLogger(__name__)


def check_collection_file(collection_file):
    """
    Checks releases for a given collection_file.

    :param int collection_file: collection file for which should be releases checked

    :raises TypeError: if there isnt CollectiionFile provided on input
    :raises AlreadyExists: if the check for a particular release already exists
    """

    if not isinstance(collection_file, CollectionFile):
        raise TypeError("collection_file is not a CollectionFile value")

    logger.info("Checking data for collection file %s", collection_file)

    items_key = None
    if (
        collection_file.collection.data_type
        and collection_file.collection.data_type["format"] == Collection.DataTypes.RELEASE_PACKAGE
    ):
        items = Release.objects.filter(collection_file_item__collection_file=collection_file).select_related(
            "data", "package_data"
        )
        items_key = "releases"
    else:
        items_key = "records"
        items = Record.objects.filter(collection_file_item__collection_file=collection_file).select_related(
            "data", "package_data"
        )

    for item in items:
        logger.debug("Checking %s form item %s", items_key, item)
        data_to_check = item.data.data
        if item.package_data:
            logger.debug("Repacking for key %s object %s", items_key, item)
            data_to_check = item.package_data.data
            data_to_check[items_key] = item.data.data

        check_result = ocds_json_output(
            "",
            "",
            schema_version="1.0",
            convert=False,
            cache_schema=True,
            file_type="json",
            json_data=data_to_check,
        )

        # eliminate nonrequired check results
        check_result.pop("releases_aggregates", None)
        check_result.pop("records_aggregates", None)

        if items_key == "releases":
            check = ReleaseCheck()
            check.release = item
        else:
            check = RecordCheck()
            check.record = item

        check.cove_output = check_result

        try:
            check.save()
        except IntegrityError:
            raise AlreadyExists("Check for a {}, item {} already exists".format(items_key, item))
