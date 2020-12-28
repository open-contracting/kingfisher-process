import logging

from ocdsmerge import Merger

from process.models import Collection, CollectionFile, CollectionFileItem, CompiledRelease, Data, Release
from process.util import get_hash

# Get an instance of a logger
logger = logging.getLogger("processor.compiler")


def compile_release(collection_id, ocid):
    logger.info("Compiling release collection_id: {} ocid: {}".format(collection_id, ocid))

    # retrieve collection including its parent from db
    collection = Collection.objects.prefetch_related("parent").get(pk=collection_id)

    # get all releases for given ocid
    releases = (
        Release.objects.filter(collection_file_item__collection_file__collection=collection)
        .filter(ocid=ocid)
        .order_by()  # avoid default order
        .prefetch_related("data")
    )

    # create array with all the data for releases
    releases_data = []
    for release in releases:
        releases_data.append(release.data.data)

    # merge data into into single compiled release
    merger = Merger()
    compiled_release = merger.create_compiled_release(releases_data)

    # create new collection file
    compiled_collection_file = CollectionFile()
    compiled_collection_file.collection = collection
    compiled_collection_file.filename = ocid
    compiled_collection_file.save()
    logger.debug("Created new collection_file {}".format(compiled_collection_file))

    # create new collection file item
    collection_file_item = CollectionFileItem()
    collection_file_item.collection_file = compiled_collection_file
    collection_file_item.number = 0
    collection_file_item.save()
    logger.debug("Created new collection_file_item {}".format(collection_file_item))

    # calculate hash to retrieve data
    compiled_release_hash = get_hash(str(compiled_release))

    try:
        # is this item already saved?
        data = Data.objects.get(hash_md5=compiled_release_hash)
        logger.debug("Found data item {} with hash: {}".format(data, compiled_release_hash))
    except (Data.DoesNotExist, Data.MultipleObjectsReturned):
        # not saved yet, create new one
        data = Data()
        data.data = compiled_release
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
