import functools
import logging

import ocdsmerge
import ocdsmerge.exceptions
from django.conf import settings
from ocdsextensionregistry.exceptions import ExtensionWarning
from ocdsextensionregistry.profile_builder import ProfileBuilder

from process.models import CollectionFile, CollectionFileItem, CollectionNote, CompiledRelease, Data
from process.util import create_note, create_warnings_note, get_or_create

logger = logging.getLogger(__name__)
format_string = "https://raw.githubusercontent.com/open-contracting-extensions/ocds_{}_extension/master/extension.json"


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

    # The master version of the lots extension depends on OCDS 1.2 or the submission terms extension.
    if format_string.format("lots") in extensions:
        extensions.add(format_string.format("submissionTerms"))

    with create_warnings_note(collection, ExtensionWarning):
        merger = _get_merger(frozenset(extensions))

    try:
        with create_warnings_note(collection, ocdsmerge.exceptions.OCDSMergeWarning):
            return merger.create_compiled_release(releases)
    except ocdsmerge.exceptions.OCDSMergeError:
        logger.exception("OCID %s can't be compiled, skipping", ocid)
        create_note(collection, CollectionNote.Level.ERROR, f"OCID {ocid} can't be compiled")


@functools.lru_cache
def _get_merger(extensions):
    tag = settings.COMPILER_OCDS_VERSION
    url = f"file://{settings.BASE_DIR / f'{tag}.zip'}"
    builder = ProfileBuilder(tag, extensions, standard_base_url=url)
    patched_schema = builder.patched_release_schema()
    return ocdsmerge.Merger(patched_schema)
