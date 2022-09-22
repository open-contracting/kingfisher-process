import logging

from django.core.management.base import BaseCommand
from django.db import transaction
from yapw.methods.blocking import ack, publish

from process.exceptions import AlreadyExists
from process.models import Collection, CompiledRelease, ProcessingStep, Release
from process.processors.compiler import compile_releases_by_ocdskit, save_compiled_release
from process.util import consume, decorator, delete_step

consume_routing_keys = ["compiler_release"]
routing_key = "release_compiler"
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Create a compiled release from the releases for a given OCID.
    """

    def handle(self, *args, **options):
        consume(callback, routing_key, consume_routing_keys, decorator=decorator)


def callback(client_state, channel, method, properties, input_message):
    ocid = input_message["ocid"]
    compiled_collection_id = input_message["compiled_collection_id"]

    with delete_step(ProcessingStep.Name.COMPILE, collection_id=compiled_collection_id, ocid=ocid):
        with transaction.atomic():
            release = compile_release(compiled_collection_id, ocid)

    message = {
        "ocid": ocid,
        "collection_id": compiled_collection_id,
        "compiled_release_id": release.pk if release else None,
    }
    publish(client_state, channel, message, routing_key)

    ack(client_state, channel, method.delivery_tag)


def compile_release(compiled_collection_id, ocid):
    collection = Collection.objects.get(pk=compiled_collection_id)

    try:
        compiled_release = collection.compiledrelease_set.get(ocid=ocid)
        raise AlreadyExists(f"Compiled release {compiled_release} already exists in collection {collection}")
    except CompiledRelease.DoesNotExist:
        pass

    releases = (
        Release.objects.filter(collection_id=collection.parent_id, ocid=ocid)
        .order_by()  # avoid default order
        .select_related("data", "package_data")
    )

    if len(releases) < 1:
        raise ValueError(f"OCID {ocid} has 0 releases.")

    data = []
    extensions = set()
    for release in releases:
        data.append(release.data.data)

        if release.package_data:
            package_extensions = release.package_data.data.get("extensions", [])
            if isinstance(package_extensions, list):
                extensions.update(package_extensions)
            else:
                logger.error("Ignored malformed extensions for release %s: %s", release, package_extensions)

    # estonia_digiwhist publishes release packages containing a single release with a "compiled" tag, and it sometimes
    # publishes the same OCID with identical data in different packages with a different `publishedDate`. The releases
    # lack a "date" field. To avoid an unnecessary error from OCDS Merge, we build the list using a set.
    #
    # https://more-itertools.readthedocs.io/en/stable/_modules/more_itertools/recipes.html#unique_everseen
    seenlist = []
    unique = []
    for d in data:
        if d not in seenlist:
            seenlist.append(d)
            unique.append(d)

    merged = compile_releases_by_ocdskit(collection, ocid, unique, extensions)
    return save_compiled_release(merged, collection, ocid)
