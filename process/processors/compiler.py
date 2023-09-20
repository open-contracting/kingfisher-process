import functools
import logging
import warnings

import ocdsmerge
import ocdsmerge.exceptions
from django.conf import settings
from ocdsextensionregistry.exceptions import ExtensionWarning
from ocdsextensionregistry.profile_builder import ProfileBuilder

from process.models import CollectionFile, CollectionFileItem, CollectionNote, CompiledRelease, Data
from process.util import create_note, get_or_create

logger = logging.getLogger(__name__)


def save_compiled_release(merged, collection, ocid):
    collection_file = CollectionFile(collection=collection, filename=f"{ocid}.json")
    collection_file.save()

    collection_file_item = CollectionFileItem(collection_file=collection_file, number=0)
    collection_file_item.save()

    data = get_or_create(Data, merged)

    release = CompiledRelease(collection=collection, collection_file_item=collection_file_item, data=data, ocid=ocid)
    release.save()

    return release


def compile_releases_by_ocdskit(collection, ocid, releases, extensions):
    # XXX: Hotfix. It otherwise takes a very long time for requests and retries to time out.
    if collection.source_id == "colombia_api":
        extensions = {extension.replace(":8443", "") for extension in extensions}

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always", category=ExtensionWarning)

        merger = _get_merger(frozenset(extensions))

        if note := [str(warning.message) for warning in w if issubclass(warning.category, ExtensionWarning)]:
            create_note(collection, CollectionNote.Level.WARNING, note)

    return merger.create_compiled_release(releases)


@functools.lru_cache
def _get_merger(extensions):
    builder = ProfileBuilder(settings.COMPILER_OCDS_VERSION, extensions)
    patched_schema = builder.patched_release_schema()
    return ocdsmerge.Merger(patched_schema)
