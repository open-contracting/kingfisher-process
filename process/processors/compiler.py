import logging

from ocdsmerge import Merger

from process.exceptions import AlreadyExists
from process.models import (
    Collection,
    CollectionFile,
    CollectionFileItem,
    CollectionFileStep,
    CompiledRelease,
    Data,
    Record,
    Release,
)
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
        .prefetch_related("data")
    )

    # raise ValueError if not such releases found
    if len(releases) < 1:
        raise ValueError("No releases with ocid {} found in parent collection.".format(ocid))

    # create array with all the data for releases
    releases_data = []
    for release in releases:
        releases_data.append(release.data.data)

    # merge data into into single compiled release
    merger = Merger()
    compiled_release_data = merger.create_compiled_release(releases_data)

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
            .prefetch_related("data")
            .get(ocid=ocid)
        )
    except Record.DoesNotExist:
        raise ValueError("No records with ocid {} found in parent collection.".format(ocid))

    # create array with all the data for releases
    records_data = []
    for release in releases:
        releases_data.append(release.data.data)

    # merge data into into single compiled release
    merger = Merger()
    compiled_release_data = merger.create_compiled_release(releases_data)
    return _save_compiled_release(compiled_release_data, collection, ocid)


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

    :param int collection_id: collection id - new compiled collection will be created based on this collection

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
            collection_file_step_count = (
                CollectionFileStep.objects.filter(collection_file__collection=collection.get_root_parent())
                .filter(name="file_worker")
                .count()
            )

            if collection_file_step_count == 0:
                compiled_collection = collection.get_compiled_collection()
                if compiled_collection.compilation_started:
                    # the compilation was already started
                    return False
                else:
                    return True
            else:
                logger.debug(
                    "Load not finished yet for collection {} - remaining {} steps.".format(
                        collection, collection_file_step_count
                    )
                )
                return False
        else:
            logger.debug("Collection {} not completely stored yet.".format(collection))
            return False

    except Collection.DoesNotExist:
        logger.info("Collection (with steps including compile) id {} not found".format(collection_id))
        return False
