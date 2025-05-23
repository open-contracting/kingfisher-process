import logging

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils.translation import gettext as t
from ocdskit.util import is_linked_release
from yapw.methods import ack, publish

from process.exceptions import AlreadyExists
from process.models import Collection, CollectionNote, CompiledRelease, ProcessingStep, Record
from process.processors.compiler import compile_releases_by_ocdskit, save_compiled_release
from process.util import consume, create_note, decorator, delete_step
from process.util import wrap as w

consume_routing_keys = ["compiler_record"]
routing_key = "record_compiler"
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = w(t("Create compiled releases from records"))

    def handle(self, *args, **options):
        consume(
            on_message_callback=callback, queue=routing_key, routing_keys=consume_routing_keys, decorator=decorator
        )


def callback(client_state, channel, method, properties, input_message):
    ocid = input_message["ocid"]
    compiled_collection_id = input_message["compiled_collection_id"]

    compiled_collection = Collection.objects.get(pk=compiled_collection_id)
    if compiled_collection.deleted_at:
        ack(client_state, channel, method.delivery_tag)
        return

    with (
        delete_step(ProcessingStep.Name.COMPILE, collection_id=compiled_collection_id, ocid=ocid),
        transaction.atomic(),
    ):
        release = compile_record(compiled_collection, ocid)

    message = {
        "collection_id": compiled_collection_id,
        "compiled_release_id": release.pk if release else None,
        "ocid": ocid,
    }
    publish(client_state, channel, message, routing_key)

    ack(client_state, channel, method.delivery_tag)


def compile_record(collection, ocid):
    try:
        compiled_release = collection.compiledrelease_set.get(ocid=ocid)
        raise AlreadyExists(f"Compiled release {compiled_release} already exists in collection {collection}")
    except CompiledRelease.DoesNotExist:
        pass

    # Similar to Model.get() https://github.com/django/django/blob/5.2/django/db/models/query.py#L609-L642
    queryset = (
        Record.objects.select_related("data", "package_data")
        .filter(collection_id=collection.parent_id, ocid=ocid)
        .order_by("id")  # unnecessary, but deterministic
    )

    count = len(queryset)
    if not count:
        raise Record.DoesNotExist(f"{Record._meta.object_name} matching query does not exist.")
    if count > 1:
        create_note(collection, CollectionNote.Level.WARNING, f"OCID {ocid} has {count} duplicates.")

    record = queryset[0]

    releases = record.data.data.get("releases", [])

    dated = []
    undated = 0
    linked = 0
    tagged = []

    for release in releases:
        if "date" in release:
            dated.append(release)
            # For example, peru_osce_bulk has a `details` field in its linked releases.
            if is_linked_release(release, maximum_properties=4):
                linked += 1
        else:
            undated += 1

        if "tag" in release and isinstance(release["tag"], list) and "compiled" in release["tag"]:
            tagged.append(release)

    # See discussion in https://github.com/open-contracting/kingfisher-process/pull/284
    if dated and not linked:
        if undated:
            note = f"OCID {ocid} has {undated} undated releases. The {len(dated)} dated releases have been compiled."
            create_note(collection, CollectionNote.Level.WARNING, note)

        extensions = set(record.package_data.data.get("extensions", []))
        if merged := compile_releases_by_ocdskit(collection, ocid, dated, extensions):
            return save_compiled_release(merged, collection, ocid)

    notes = []
    if linked:
        notes.append(
            f"OCID {ocid} has {linked} linked releases among {len(dated)} dated releases and {len(releases)} releases."
        )
    elif undated:
        notes.append(f"OCID {ocid} has {len(releases)} releases, all undated.")
    else:
        notes.append(f"OCID {ocid} has 0 releases.")

    if compiled_release := record.data.data.get("compiledRelease", []):
        notes.append("Its compiledRelease was used.")
        create_note(
            collection,
            # Use INFO level if all releases are dated and linked.
            CollectionNote.Level.INFO if linked == len(dated) == len(releases) > 0 else CollectionNote.Level.WARNING,
            notes,
        )
        return save_compiled_release(compiled_release, collection, ocid)

    if tagged:
        if len(tagged) > 1:
            notes.append("Its first release tagged 'compiled' was used.")
        else:
            notes.append("Its only release tagged 'compiled' was used.")
        create_note(collection, CollectionNote.Level.WARNING, notes)
        return save_compiled_release(tagged[0], collection, ocid)

    notes.append("It has no compiledRelease and no releases tagged 'compiled'. It was not compiled.")
    create_note(collection, CollectionNote.Level.ERROR, notes)
    return None
