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
    The worker is responsible for the compilation of particular releases.
    Consumes messages with an ocid and collection_id which should be compiled.
    The whole structure of CollectionFile, CollectionFileItem, and CompiledRelease
    is created and saved.
    It's safe to run multiple workers of this type at the same type.
    """

    def handle(self, *args, **options):
        consume(callback, routing_key, consume_routing_keys, decorator=decorator, prefetch_count=20)


@transaction.atomic
def callback(client_state, channel, method, properties, input_message):
    ocid = input_message["ocid"]
    collection_id = input_message["collection_id"]
    compiled_collection_id = input_message["compiled_collection_id"]

    release = compile_release(collection_id, ocid)

    delete_step(ProcessingStep.Types.COMPILE, collection_id=compiled_collection_id, ocid=ocid)

    message = {
        "ocid": ocid,
        "collection_id": compiled_collection_id,
        "compiled_release_id": release.pk if release else None,
    }
    publish(client_state, channel, message, routing_key)

    ack(client_state, channel, method.delivery_tag)


def compile_release(collection_id, ocid):
    """
    Compiles releases of given ocid in a "parent" collection. Creates the whole structure in the "proper" transformed
    collection - CollectionFile, CollectionFileItems, Data, and CompiledRelease.

    :param int collection_id: collection id to which the ocid belongs to
    :param str ocid: ocid of release whiich should be compiled

    :returns: compiled release
    :rtype: CompiledRelease
    """

    logger.info("Compiling release collection_id: %s ocid: %s", collection_id, ocid)

    collection = Collection.objects.filter(parent_id=collection_id).get(parent__steps__contains="compile")

    try:
        compiled_release = CompiledRelease.objects.filter(collection=collection).get(ocid=ocid)
        raise AlreadyExists("CompiledRelease {} for Collection {} already exists".format(compiled_release, collection))
    except CompiledRelease.DoesNotExist:
        pass

    releases = (
        Release.objects.filter(collection_id=collection.parent_id, ocid=ocid)
        .order_by()  # avoid default order
        .select_related("data", "package_data")
    )

    if len(releases) < 1:
        raise ValueError("No releases with ocid {} found in parent collection.".format(ocid))

    releases_data = []
    extensions = set()
    for release in releases:
        releases_data.append(release.data.data)

        # collect all extensions used
        if release.package_data:
            package_data_extensions = release.package_data.data.get("extensions", [])
            if isinstance(package_data_extensions, list):
                extensions.update(package_data_extensions)
            else:
                logger.error(
                    "Package data for release %s contains malformed extensions %s, skipping.",
                    release,
                    package_data_extensions,
                )

    # estonia_digiwhist publishes release packages containing a single release with a "compiled" tag, and it sometimes
    # publishes the same OCID with identical data in different packages with a different `publishedDate`. The releases
    # lack a "date" field. To avoid an unnecessary error from OCDS Merge, we build the list using a set.
    #
    # https://more-itertools.readthedocs.io/en/stable/_modules/more_itertools/recipes.html#unique_everseen
    seenlist = []
    releases_data_unique = []
    for d in releases_data:
        if d not in seenlist:
            seenlist.append(d)
            releases_data_unique.append(d)

    # merge data into into single compiled release
    compiled_release_data = compile_releases_by_ocdskit(collection, ocid, releases_data_unique, extensions)

    return save_compiled_release(compiled_release_data, collection, ocid)
