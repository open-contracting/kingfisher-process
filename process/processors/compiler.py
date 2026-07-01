import functools
import itertools
import logging
import warnings
from collections import Counter
from operator import itemgetter

from django.conf import settings
from ocdsextensionregistry import ProfileBuilder
from ocdsextensionregistry.exceptions import ExtensionWarning
from ocdsmerge_rs import Merger
from ocdsmerge_rs.exceptions import DuplicateIdValueWarning, MergeError, MergeWarning

from process.models import CollectionFile, CollectionNote, CompiledRelease, Data, Release
from process.util import create_note, get_extensions, get_or_create

logger = logging.getLogger(__name__)
WARNING = CollectionNote.Level.WARNING


def compile_release_batch(collection, ocids):
    """
    Compile the OCIDs in bulk.

    Idempotent under duplicate message delivery, provided this is called within a transaction that also deletes the
    COMPILE steps, so that the compiled release insertions and processing step deletions commit together.

    Two messages never partially overlap on OCIDs: the compiler command sets compilation_started with an optimistic
    lock and publishes each collection's OCIDs once, partitioned into disjoint batches.

    So, the only duplicate is a redelivery of the same message, in which case:

    - A sequential redelivery is a no-op: already-compiled OCIDs are filtered out below, before any insert.
    - A concurrent redelivery is safe: CollectionFile's unique (collection, filename) constraint lets only one of the
      two transactions commit; the other raises IntegrityError, rolls back entirely (creating nothing), and is nack'ed
      without requeue. The committed transaction deletes every COMPILE step, and the collection still completes.

    :param collection: the compiled collection
    :param ocids: the OCIDs to compile
    :returns: the OCIDs that were compiled
    """
    already_exists = set(
        CompiledRelease.objects.filter(collection=collection, ocid__in=ocids).values_list("ocid", flat=True)
    )
    does_not_exist = []
    for ocid in ocids:
        if ocid in already_exists:
            logger.error("Compiled release %s already exists in collection %s", ocid, collection)
        else:
            does_not_exist.append(ocid)
    ocids = does_not_exist
    if not ocids:
        return []

    rows = (
        Release.objects.filter(collection_id=collection.parent_id, ocid__in=ocids)
        .order_by("ocid", "release_date")
        .values_list("ocid", "data__data", "package_data__data")
    )

    # The rows are ordered by OCID, so merge each OCID's releases as its rows stream in, to hold only one OCID's
    # releases in memory at a time. Some OCIDs have thousands of releases (#460).
    merged_batch = []
    seen_ocids = set()
    for ocid, group in itertools.groupby(rows.iterator(), key=itemgetter(0)):
        seen_ocids.add(ocid)
        releases = []
        extensions = set()
        for _, release, package in group:
            releases.append(release)
            if package:
                extensions.update(get_extensions(package))
        if merged := compile_releases_by_ocdskit(collection, ocid, releases, extensions):
            merged_batch.append((ocid, merged))

    # OCIDs with no releases do not appear in the query results.
    for ocid in ocids:
        if ocid not in seen_ocids:
            create_note(collection, CollectionNote.Level.ERROR, f"OCID {ocid} has 0 releases.")

    if merged_batch:
        save_compiled_releases(collection, merged_batch)

    return [ocid for ocid, _ in merged_batch]


def save_compiled_releases(collection, merged_batch):
    """
    Bulk-create the compiled releases for a collection.

    :param merged_batch: a list of ``(ocid, merged)`` pairs
    """
    collection_files = [CollectionFile(collection=collection, filename=f"{ocid}.json") for ocid, _ in merged_batch]
    CollectionFile.objects.bulk_create(collection_files)

    # Like process.management.commands.file_worker._store_data()
    if settings.DEDUPLICATE_DATA:
        data_objects = [get_or_create(Data, merged) for _, merged in merged_batch]
    else:
        data_objects = [Data(hash_md5="", data=merged) for _, merged in merged_batch]
        Data.objects.bulk_create(data_objects)

    CompiledRelease.objects.bulk_create(
        [
            CompiledRelease(
                collection=collection,
                collection_file=collection_file,
                data=data,
                ocid=ocid,
                release_date=merged.get("date") or "",
            )
            for (ocid, merged), collection_file, data in zip(merged_batch, collection_files, data_objects, strict=True)
        ]
    )


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
        create_note(collection, WARNING, str(w.message), data={"type": w.category.__name__})

    try:
        with warnings.catch_warnings(record=True, action="always", category=MergeWarning) as wlist:
            merged = merger.create_compiled_release(releases)
    except MergeError as e:
        logger.exception("OCID %s can't be compiled, skipping", ocid)
        create_note(
            collection,
            CollectionNote.Level.ERROR,
            f"OCID {ocid} can't be compiled",
            data={"type": type(e).__name__, "message": str(e), **vars(e)},
        )
    else:
        notes = []
        paths = Counter()
        for w in filter_warnings(wlist, MergeWarning):
            if isinstance(w.message, DuplicateIdValueWarning):
                # Aggregate DuplicateIdValueWarning, because it can be issued many times.
                notes.append(str(w.message))
                paths[w.message.path] += 1
            else:
                create_note(collection, WARNING, str(w.message), data={"type": w.category.__name__, **vars(w.message)})

        if notes:
            create_note(collection, WARNING, notes, data={"type": "DuplicateIdValueWarning", "paths": dict(paths)})

        return merged


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
