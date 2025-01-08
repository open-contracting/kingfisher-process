import functools
import logging

import ocdsmerge
import ocdsmerge.exceptions
from django.conf import settings
from ocdsextensionregistry import ProfileBuilder
from ocdsextensionregistry.exceptions import ExtensionWarning

from process.models import CollectionFile, CollectionNote, CompiledRelease, Data
from process.util import create_note, create_warnings_note, get_or_create

logger = logging.getLogger(__name__)


def save_compiled_release(merged, collection, ocid):
    collection_file = CollectionFile(collection=collection, filename=f"{ocid}.json")
    collection_file.save()

    data = get_or_create(Data, merged)

    release = CompiledRelease(
        collection=collection,
        collection_file=collection_file,
        data=data,
        ocid=ocid,
        release_date=merged.get("date") or "",
    )
    release.save()

    return release


def compile_releases_by_ocdskit(collection, ocid, releases, extensions):
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
    # Security: Potential SSRF via extension URLs (within OCDS publication).
    builder = ProfileBuilder(tag, extensions, standard_base_url=url)
    patched_schema = builder.patched_release_schema()
    return ocdsmerge.Merger(patched_schema)
