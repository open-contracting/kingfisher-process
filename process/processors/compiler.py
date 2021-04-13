import logging

import ocdsmerge
from django.conf import settings
from ocdsextensionregistry.profile_builder import ProfileBuilder
from ocdskit.util import is_linked_release

from process.exceptions import AlreadyExists
from process.models import (Collection, CollectionFile, CollectionFileItem, CompiledRelease, Data, ProcessingStep,
                            Record, Release)
from process.util import get_hash

# Get an instance of a logger
logger = logging.getLogger("processor.compiler")


def compile_release(collection_id, ocid):
    """
    Compiles releases of given ocid in a "parent" collection. Creates the whole structure in the "proper" transformed
    collection - CollectionFile, CollectionFileItems, Data, and CompiledRelease.

    :param int collection_id: collection id to which the ocid belongs to
    :param str ocid: ocid of release whiich should be compiled

    :returns: compiled release
    :rtype: CompiledRelease

    :raises TypeError: if there arent integers provided on input
    :raises ValueError: if there are no items of such id (collections/ocid)
    :raises ValueError: if the compiled release already exists
    :raises ValueError: if there are no releases to compile in the source/parent collection
    """

    # validate input
    if not isinstance(collection_id, int):
        raise TypeError("collection_id is not an int value")
    if not isinstance(ocid, str):
        raise TypeError("ocid is not a string value")

    logger.info("Compiling release collection_id: {} ocid: {}".format(collection_id, ocid))

    # retrieve collection including its parent from db
    try:
        collection = Collection.objects.filter(parent=collection_id).get(parent__steps__contains="compile")
        if not collection:
            # no collection found
            raise ValueError("Compiled collection with parent collection id {} not found".format(collection_id))
    except Collection.DoesNotExist:
        raise ValueError("Compiled collection with parent collection id {} not found".format(collection_id))

    # check, whether the ocid wasnt already compiled
    try:
        compiled_release = CompiledRelease.objects.filter(ocid__exact=ocid).get(
            collection_file_item__collection_file__collection=collection
        )
        raise AlreadyExists(
            "CompiledRelease {} for a collection {} already exists".format(compiled_release, collection)
        )
    except CompiledRelease.DoesNotExist:
        # happy day - compiled release should not exist
        logger.debug("compiled_release with ocid {} does not exist, we can compile that and store it".format(ocid))

    # get all releases for given ocid
    releases = (
        Release.objects.filter(collection_file_item__collection_file__collection=collection.parent)
        .filter(ocid=ocid)
        .order_by()  # avoid default order
        .prefetch_related("package_data")
        .prefetch_related("data")
    )

    # raise ValueError if not such releases found
    if len(releases) < 1:
        raise ValueError("No releases with ocid {} found in parent collection.".format(ocid))

    # create array with all the data for releases
    releases_data = []
    extensions = []
    for release in releases:
        releases_data.append(release.data.data)

        # collect all extensions used
        if release.package_data:
            extensions = list(set(extensions + release.package_data.data.get("extensions", [])))

    # merge data into into single compiled release
    compiled_release_data = _compile_releases_by_ocdskit(ocid, releases_data, extensions)

    return _save_compiled_release(compiled_release_data, collection, ocid)


def compile_record(collection_id, ocid):
    """
    Compiles records of given ocid in a "parent" collection. Creates the whole structure in the "proper" transformed
    collection - CollectionFile, CollectionFileItems, Data, and CompiledRelease.

    :param int collection_id: collection id to which the ocid belongs to
    :param str ocid: ocid of release whiich should be compiled

    :returns: compiled release
    :rtype: CompiledRelease

    :raises TypeError: if there arent integers provided on input
    :raises ValueError: if there are no items of such id (collections/ocid)
    :raises ValueError: if the compiled release already exists
    :raises ValueError: if there are no records to compile in the source/parent collection
    """

    # validate input
    if not isinstance(collection_id, int):
        raise TypeError("collection_id is not an int value")
    if not isinstance(ocid, str):
        raise TypeError("ocid is not a string value")

    logger.info("Compiling record collection_id: {} ocid: {}".format(collection_id, ocid))

    # retrieve collection including its parent from db
    try:
        collection = Collection.objects.filter(parent=collection_id).get(parent__steps__contains="compile")
        if not collection:
            # no collection found
            raise ValueError("Compiled collection with parent collection id {} not found".format(collection_id))
    except Collection.DoesNotExist:
        raise ValueError("Compiled collection with parent collection id {} not found".format(collection_id))

    # check, whether the ocid wasnt already compiled
    try:
        compiled_release = CompiledRelease.objects.filter(ocid__exact=ocid).get(
            collection_file_item__collection_file__collection=collection
        )

        raise AlreadyExists(
            "CompiledRelease {} for a collection {} already exists".format(compiled_release, collection)
        )
    except CompiledRelease.DoesNotExist:
        # happy day - compiled release should not exist
        logger.debug("compiled_release with ocid {} does not exist, we can compile that and store it".format(ocid))

    # get records for given ocid
    try:
        record = (
            Record.objects.filter(collection_file_item__collection_file__collection=collection.parent)
            .select_related("data")
            .select_related("package_data")
            .get(ocid=ocid)
        )
    except Record.DoesNotExist:
        raise ValueError("No records with ocid {} found in parent collection.".format(ocid))

    # create array with all the data for releases
    releases = record.data.data.get("releases", [])
    releases_with_date, releases_without_date = _check_dates_in_releases(releases)

    # Can we compile ourselves?
    releases_linked = [r for r in releases_with_date if is_linked_release(r)]
    if releases_with_date and not releases_linked:
        # We have releases with date fields and none are linked (have URL's).
        # We can compile them ourselves.
        # (Checking releases_with_date here and not releases means that a record with
        #  a compiledRelease and releases with no dates will be processed by using the compiledRelease,
        #  so we still have some data)
        if releases_without_date:
            logger.warning(
                "This OCID {} had some releases without a date element. We have compiled all other releases.".format(
                    ocid
                )
            )

        extensions = record.package_data.data.get("extensions", [])
        compiled_release_data = _compile_releases_by_ocdskit(ocid, releases_with_date, extensions)
        return _save_compiled_release(compiled_release_data, collection, ocid)

    if releases_without_date:
        logger.warning("This OCID had some releases without a date element.")

    # Is there a compiledRelease?
    compiled_release = record.get("compiledRelease")
    if compiled_release:
        logger.warning(
            """
            This record {} already had a compiledRelease in the record!
            It was passed through this transform unchanged.""".format(
                record
            )
        )

        return _save_compiled_release(compiled_release, collection, ocid)

    # Is there a release tagged 'compiled'?
    releases_compiled = [x for x in releases if "tag" in x and isinstance(x["tag"], list) and "compiled" in x["tag"]]

    if len(releases_compiled) > 1:
        # If more than one, pick one at random. and log that.
        logger.warning(
            """
            This record {} already has multiple compiled releases in the releases array!
            The compiled release to pass through was selected arbitrarily.""".format(
                record
            )
        )
        return _save_compiled_release(releases_compiled[0], collection, ocid)

    elif len(releases_compiled) == 1:
        # There is just one compiled release - pass it through unchanged, and log that.
        logger.warning(
            """
            This record {} already has multiple compiled releases in the releases array!
            It was passed through this transform unchanged.""".format(
                record
            )
        )

        return _save_compiled_release(releases_compiled[0], collection, ocid)

    else:
        # We can't process this ocid. Warn of that.
        logger.error(
            """
            Record {} could not be compiled because at least one release in the releases array is
            a linked release or there are no releases with dates, and the record has
            neither a compileRelease nor a release with a tag of "compiled".""".format(
                record
            )
        )

    return None


def _save_compiled_release(compiled_release_data, collection, ocid):
    # create new collection file
    compiled_collection_file = CollectionFile()
    compiled_collection_file.collection = collection
    compiled_collection_file.filename = "{}.json".format(ocid)
    compiled_collection_file.save()
    logger.debug("Created new collection_file {}".format(compiled_collection_file))

    # create new collection file item
    collection_file_item = CollectionFileItem()
    collection_file_item.collection_file = compiled_collection_file
    collection_file_item.number = 0
    collection_file_item.save()
    logger.debug("Created new collection_file_item {}".format(collection_file_item))

    # calculate hash to retrieve data
    compiled_release_hash = get_hash(str(compiled_release_data))

    try:
        # is this item already saved?
        data = Data.objects.get(hash_md5=compiled_release_hash)
        logger.debug("Found data item {} with hash: {}".format(data, compiled_release_hash))
    except (Data.DoesNotExist, Data.MultipleObjectsReturned):
        # not saved yet, create new one
        data = Data()
        data.data = compiled_release_data
        data.hash_md5 = compiled_release_hash
        data.save()
        logger.debug("Created new data item {}".format(data))

    # create and store release
    release = CompiledRelease()
    release.collection = collection
    release.collection_file_item = collection_file_item
    release.data = data
    release.ocid = ocid
    release.save()

    logger.debug("Stored compiled release {}".format(release))

    return release


def compilable(collection_id):
    """
    Checks whether the collection
        * should be compiled (compile in steps)
        * could be compiled (load complete)
        * already wasn't compiled

    :param int collection_id: collection id - to be checked

    :returns: true if the collection can be created
    :rtype: bool

    :raises ValueError: if there is no collection with such collection_id or
    """

    # validate input
    if not isinstance(collection_id, int):
        raise TypeError("collection_id is not an int value")

    try:
        collection = Collection.objects.filter(id=collection_id).get(steps__contains="compile")

        if collection.data_type and collection.data_type["format"] == Collection.DataTypes.RECORD_PACKAGE:
            # records can be processed immediately
            return True

        if collection.store_end_at is not None:
            processing_step_count = (
                ProcessingStep.objects.filter(collection_file__collection=collection.get_root_parent())
                .filter(name=ProcessingStep.Types.LOAD)
                .count()
            )

            if processing_step_count == 0:
                compiled_collection = collection.get_compiled_collection()
                if compiled_collection.compilation_started:
                    # the compilation was already started
                    return False
                else:
                    return True
            else:
                logger.debug(
                    "Load not finished yet for collection {} - remaining {} steps.".format(
                        collection, processing_step_count
                    )
                )
                return False
        else:
            logger.debug("Collection {} not completely stored yet.".format(collection))
            return False

    except Collection.DoesNotExist:
        logger.info("Collection (with steps including compile) id {} not found".format(collection_id))
        return False


def _check_dates_in_releases(releases):
    releases_with_date = [r for r in releases if "date" in r]
    releases_without_date = [r for r in releases if "date" not in r]
    return releases_with_date, releases_without_date


def _compile_releases_by_ocdskit(ocid, releases, extensions):
    try:
        try:
            builder = ProfileBuilder(settings.COMPILER_OCDS_VERSION, extensions)
            schema = builder.patched_release_schema()
            merger = ocdsmerge.Merger(schema)
            out = merger.create_compiled_release(releases)
            return out
        except Exception as e:
            logger.error("Unable to compile with extensions, trying without them {}".format(e))
            logger.info("Trying to compile without extensions.")

            builder = ProfileBuilder(settings.COMPILER_OCDS_VERSION, [])
            schema = builder.patched_release_schema()
            merger = ocdsmerge.Merger(schema)
            out = merger.create_compiled_release(releases)
            return out

    except Exception as e:
        logger.exception("OCID {} could not be compiled because merge library threw an error: ".format(ocid), e)

        raise e
