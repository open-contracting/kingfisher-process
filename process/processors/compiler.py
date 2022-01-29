import logging

import ocdsmerge
import ocdsmerge.exceptions
from django.conf import settings
from ocdsextensionregistry.profile_builder import ProfileBuilder

from process.models import CollectionFile, CollectionFileItem, CompiledRelease, Data
from process.util import get_hash

logger = logging.getLogger(__name__)


def save_compiled_release(compiled_release_data, collection, ocid):
    collection_file = CollectionFile(collection=collection, filename=f"{ocid}.json")
    collection_file.save()

    collection_file_item = CollectionFileItem(collection_file=collection_file, number=0)
    collection_file_item.save()

    hash_md5 = get_hash(str(compiled_release_data))

    try:
        data = Data.objects.get(hash_md5=hash_md5)
    except (Data.DoesNotExist, Data.MultipleObjectsReturned):
        data = Data(data=compiled_release_data, hash_md5=hash_md5)
        data.save()

    release = CompiledRelease(collection=collection, collection_file_item=collection_file_item, data=data, ocid=ocid)
    release.save()

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
