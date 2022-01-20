import logging

import ocdsmerge
import ocdsmerge.exceptions
from django.conf import settings
from ocdsextensionregistry.profile_builder import ProfileBuilder

from process.models import CollectionFile, CollectionFileItem, CompiledRelease, Data
from process.util import get_hash

logger = logging.getLogger(__name__)


def save_compiled_release(compiled_release_data, collection, ocid):
    # create new collection file
    compiled_collection_file = CollectionFile()
    compiled_collection_file.collection = collection
    compiled_collection_file.filename = "{}.json".format(ocid)
    compiled_collection_file.save()
    logger.debug("Created new collection_file %s", compiled_collection_file)

    # create new collection file item
    collection_file_item = CollectionFileItem()
    collection_file_item.collection_file = compiled_collection_file
    collection_file_item.number = 0
    collection_file_item.save()
    logger.debug("Created new collection_file_item %s", collection_file_item)

    # calculate hash to retrieve data
    compiled_release_hash = get_hash(str(compiled_release_data))

    try:
        # is this item already saved?
        data = Data.objects.get(hash_md5=compiled_release_hash)
        logger.debug("Found data item %s with hash: %s", data, compiled_release_hash)
    except (Data.DoesNotExist, Data.MultipleObjectsReturned):
        # not saved yet, create new one
        data = Data()
        data.data = compiled_release_data
        data.hash_md5 = compiled_release_hash
        data.save()
        logger.debug("Created new data item %s", data)

    # create and store release
    release = CompiledRelease()
    release.collection = collection
    release.collection_file_item = collection_file_item
    release.data = data
    release.ocid = ocid
    release.save()

    logger.debug("Stored compiled release %s", release)

    return release


def compile_releases_by_ocdskit(collection, ocid, releases, extensions):
    try:
        schema = _get_patched_release_schema(extensions)
    except Exception:
        # TODO Replace this with more specific exceptions and reduce logging level as appropriate, once more errors
        # appear in Sentry.
        logger.exception("Using unpatched schema after failing to patch schema")
        schema = _get_patched_release_schema([])

    merger = ocdsmerge.Merger(schema)
    return merger.create_compiled_release(releases)


def _get_patched_release_schema(extensions):
    builder = ProfileBuilder(settings.COMPILER_OCDS_VERSION, extensions)
    return builder.patched_release_schema()
