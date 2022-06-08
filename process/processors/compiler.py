import functools
import logging

import ocdsmerge
import ocdsmerge.exceptions
from django.conf import settings
from ocdsextensionregistry.profile_builder import ProfileBuilder

from process.models import CollectionFile, CollectionFileItem, CompiledRelease, Data
from process.util import get_hash

logger = logging.getLogger(__name__)


def save_compiled_release(merged, collection, ocid):
    collection_file = CollectionFile(collection=collection, filename=f"{ocid}.json")
    collection_file.save()

    collection_file_item = CollectionFileItem(collection_file=collection_file, number=0)
    collection_file_item.save()

    hash_md5 = get_hash(str(merged))

    try:
        data = Data.objects.get(hash_md5=hash_md5)
    except (Data.DoesNotExist, Data.MultipleObjectsReturned):
        data = Data(data=merged, hash_md5=hash_md5)
        data.save()

    release = CompiledRelease(collection=collection, collection_file_item=collection_file_item, data=data, ocid=ocid)
    release.save()

    return release


def compile_releases_by_ocdskit(collection, ocid, releases, extensions):
    # XXX: Hotfix. It otherwise takes a very long time for requests and retries to time out.
    if collection.source_id == "colombia_api":
        extensions = {extension.replace(":8443", "") for extension in extensions}

    try:
        merger = _get_merger(frozenset(extensions))
    # TODO Replace this with more specific exceptions, e.g. failed HTTP requests, bad ZIP files, etc.
    # and reduce logging level as appropriate, once more errors appear in Sentry.
    except Exception:
        logger.exception("Using unpatched schema after failing to patch schema")
        merger = _get_merger(frozenset())

    return merger.create_compiled_release(releases)


@functools.lru_cache
def _get_merger(extensions):
    builder = ProfileBuilder(settings.COMPILER_OCDS_VERSION, extensions)
    patched_schema = builder.patched_release_schema()
    return ocdsmerge.Merger(patched_schema)
