import logging

from django.db.utils import IntegrityError
from libcoveocds.api import ocds_json_output

from process.exceptions import AlreadyExists
from process.models import CollectionFile, Release, ReleaseCheck

# Get an instance of a logger
logger = logging.getLogger("processor.checker")


def check_releases(collection_file_id):
    """
    Checks releases for a given collection_file.

    :param int collection_file_id: collection file id for which should be releases checked

    :raises TypeError: if there arent integers provided on input
    :raises ValueError: if there is no item of such id
    :raises AlreadyExists: if the check for a particular release already exists
    """

    # validate input
    if not isinstance(collection_file_id, int):
        raise TypeError("collection_file_id is not an int value")

    try:
        collection_file = CollectionFile.objects.get(id=collection_file_id)
        logger.info("Checking releases for collection file {}".format(collection_file))

        releases = Release.objects.filter(collection_file_item__collection_file=collection_file)

        for release in releases:
            logger.debug("Checking release {}".format(release))
            check_result = ocds_json_output(
                "",
                "",
                schema_version="1.0",
                convert=False,
                cache_schema=True,
                file_type="json",
                json_data=release.data.data,
            )

            # eliminate nonrequired check results
            check_result.pop("releases_aggregates", None)
            check_result.pop("records_aggregates", None)

            releaseCheck = ReleaseCheck()
            releaseCheck.cove_output = check_result
            releaseCheck.release = release
            try:
                releaseCheck.save()
            except IntegrityError:
                raise AlreadyExists("Check for a release {} already exists".format(release))

    except CollectionFile.DoesNotExist:
        raise ValueError("Collection file id {} not found".format(collection_file_id))
