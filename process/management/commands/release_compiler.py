import logging

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils.translation import gettext as t
from yapw.methods import ack, publish

from process.exceptions import AlreadyExists
from process.models import Collection, CollectionNote, CompiledRelease, ProcessingStep, Release
from process.processors.compiler import compile_releases_by_ocdskit, save_compiled_release
from process.util import consume, create_note, decorator, deleting_step, get_extensions
from process.util import wrap as w

consume_routing_keys = ["compiler_release"]
routing_key = "release_compiler"
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = w(t("Create compiled releases from releases with the same OCID"))

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
        deleting_step(ProcessingStep.Name.COMPILE, collection_id=compiled_collection_id, ocid=ocid),
        transaction.atomic(),
    ):
        release = compile_release(compiled_collection, ocid)

    message = {
        "collection_id": compiled_collection_id,
        "compiled_release_id": release.pk if release else None,
        "ocid": ocid,
    }
    publish(client_state, channel, message, routing_key)

    ack(client_state, channel, method.delivery_tag)


def compile_release(collection, ocid):
    try:
        compiled_release = collection.compiledrelease_set.get(ocid=ocid)
        raise AlreadyExists(f"Compiled release {compiled_release} already exists in collection {collection}")
    except CompiledRelease.DoesNotExist:
        pass

    releases = (
        Release.objects.select_related("data", "package_data")
        .filter(collection_id=collection.parent_id, ocid=ocid)
        .order_by("release_date")
        .values_list("data__data", "package_data__data")
    )

    if not releases.count():
        create_note(collection, CollectionNote.Level.ERROR, f"OCID {ocid} has 0 releases.")
        return None

    data = []
    extensions = set()
    for release, package in releases.iterator(chunk_size=100):  # default 2000
        data.append(release)
        if package:
            extensions.update(get_extensions(package))

    # estonia_digiwhist publishes release packages containing a single release with a "compiled" tag, and it sometimes
    # publishes the same OCID with identical data in different packages with a different `publishedDate`. The releases
    # lack a "date" field. To avoid an unnecessary error from OCDS Merge, we build a unique list. We cannot use set(),
    # as the elements of the set are dicts, which are unhashable.
    #
    # https://more-itertools.readthedocs.io/en/stable/_modules/more_itertools/recipes.html#unique_everseen
    seenlist = []
    unique = []
    for d in data:
        if d not in seenlist:
            seenlist.append(d)
            unique.append(d)

    if merged := compile_releases_by_ocdskit(collection, ocid, unique, extensions):
        return save_compiled_release(merged, collection, ocid)

    return None
