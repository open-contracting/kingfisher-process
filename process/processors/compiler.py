import functools
import logging
import warnings
from collections import Counter

from django.conf import settings
from ocdsextensionregistry import ProfileBuilder
from ocdsextensionregistry.exceptions import ExtensionWarning
from ocdsmerge_rs import Merger
from ocdsmerge_rs.exceptions import DuplicateIdValueWarning, MergeError, MergeWarning

from process.models import CollectionFile, CollectionNote, CompiledRelease, Data
from process.util import create_note, get_or_create

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
    with warnings.catch_warnings(record=True, action="always", category=ExtensionWarning) as wlist:
        merger = _get_merger(frozenset(extensions))

    for w in filter_warnings(wlist, ExtensionWarning):
        create_note(collection, CollectionNote.Level.WARNING, str(w.message), data={"type": w.category.__name__})

    try:
        with warnings.catch_warnings(record=True, action="always", category=MergeWarning) as wlist:
            return merger.create_compiled_release(releases)

        notes = []
        paths = Counter()
        for w in filter_warnings(wlist, DuplicateIdValueWarning):  # DuplicateIdValueWarning is the only MergeWarning
            notes.append(str(w.message))
            paths[w.message.path] += 1

        if notes:
            create_note(
                collection,
                CollectionNote.Level.WARNING,
                notes,
                data={"type": "DuplicateIdValueWarning", "paths": dict(paths)},
            )
    except MergeError as e:
        logger.exception("OCID %s can't be compiled, skipping", ocid)
        create_note(
            collection,
            CollectionNote.Level.ERROR,
            f"OCID {ocid} can't be compiled",
            data={"type": type(e).__name__, "message": str(e), **vars(e)},
        )


@functools.lru_cache
def _get_merger(extensions):
    tag = settings.COMPILER_OCDS_VERSION
    url = f"file://{settings.BASE_DIR / f'{tag}.zip'}"
    # Security: Potential SSRF via extension URLs (within OCDS publication).
    builder = ProfileBuilder(tag, extensions, standard_base_url=url)
    patched_schema = builder.patched_release_schema()
    return Merger(rules=Merger.get_rules(Merger.dereference(patched_schema)))


def filter_warnings(wlist, category):
    """Yield warnings that match category, re-emitting non-matching warnings."""
    for w in wlist:
        if issubclass(w.category, category):
            yield w
        else:
            warnings.warn_explicit(w.message, w.category, w.filename, w.lineno, source=w.source)
