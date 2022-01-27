import logging

from django.core.management.base import BaseCommand
from django.db import transaction
from ocdskit.util import is_linked_release
from yapw.methods.blocking import ack, publish

from process.exceptions import AlreadyExists
from process.models import Collection, CompiledRelease, ProcessingStep, Record
from process.processors.compiler import compile_releases_by_ocdskit, save_compiled_release
from process.util import consume, decorator, delete_step

consume_routing_keys = ["compiler_record"]
routing_key = "record_compiler"
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    The worker is responsible for the compilation of particular records.
    Consumes messages with an ocid and collection_id which should be compiled.
    The whole structure of CollectionFile, CollectionFileItem, and CompiledRelease
    is created and saved.
    It's safe to run multiple workers of this type at the same type.
    """

    def handle(self, *args, **options):
        consume(
            callback,
            routing_key,
            consume_routing_keys,
            decorator=decorator,
        )


@transaction.atomic
def callback(client_state, channel, method, properties, input_message):
    ocid = input_message["ocid"]
    collection_id = input_message["collection_id"]
    compiled_collection_id = input_message["compiled_collection_id"]

    release = compile_record(collection_id, ocid)

    delete_step(ProcessingStep.Types.COMPILE, collection_id=compiled_collection_id, ocid=ocid)

    message = {
        "ocid": ocid,
        "collection_id": compiled_collection_id,
        "compiled_release_id": release.pk if release else None,
    }
    publish(client_state, channel, message, routing_key)

    ack(client_state, channel, method.delivery_tag)


def compile_record(collection_id, ocid):
    """
    Compiles records of given ocid in a "parent" collection. Creates the whole structure in the "proper" transformed
    collection - CollectionFile, CollectionFileItems, Data, and CompiledRelease.

    :param int collection_id: collection id to which the ocid belongs to
    :param str ocid: ocid of release whiich should be compiled

    :returns: compiled release
    :rtype: CompiledRelease
    """

    logger.info("Compiling record collection_id: %s ocid: %s", collection_id, ocid)

    collection = Collection.objects.filter(parent_id=collection_id).get(parent__steps__contains="compile")

    try:
        compiled_release = CompiledRelease.objects.filter(collection=collection).get(ocid=ocid)
        raise AlreadyExists("CompiledRelease {} for Collection {} already exists".format(compiled_release, collection))
    except CompiledRelease.DoesNotExist:
        pass

    record = (
        Record.objects.filter(collection_id=collection.parent_id).select_related("data", "package_data").get(ocid=ocid)
    )

    # create array with all the data for releases
    releases = record.data.data.get("releases", [])
    releases_with_date = [r for r in releases if "date" in r]
    releases_without_date = [r for r in releases if "date" not in r]

    # Can we compile ourselves?
    releases_linked = [r for r in releases_with_date if is_linked_release(r)]
    if releases_with_date and not releases_linked:
        # We have releases with date fields and none are linked (have URL's).
        # We can compile them ourselves.
        # (Checking releases_with_date here and not releases means that a record with
        #  a compiledRelease and releases with no dates will be processed by using the compiledRelease,
        #  so we still have some data)
        if releases_without_date:
            logger.warning(
                "This OCID %s had some releases without a date element. We have compiled all other releases.", ocid
            )

        extensions = record.package_data.data.get("extensions", [])
        compiled_release_data = compile_releases_by_ocdskit(collection, ocid, releases_with_date, extensions)
        return save_compiled_release(compiled_release_data, collection, ocid)

    if releases_without_date:
        logger.warning("This OCID had some releases without a date element.")

    # Is there a compiledRelease?
    compiled_release = record.data.data.get("compiledRelease", [])
    if compiled_release:
        logger.warning(
            "This record %s already had a compiledRelease in the record! "
            "It was passed through this transform unchanged.",
            record,
        )

        return save_compiled_release(compiled_release, collection, ocid)

    # Is there a release tagged 'compiled'?
    releases_compiled = [x for x in releases if "tag" in x and isinstance(x["tag"], list) and "compiled" in x["tag"]]

    if len(releases_compiled) > 1:
        # If more than one, pick one at random. and log that.
        logger.warning(
            "This record %s already has multiple compiled releases in the releases array!"
            "The compiled release to pass through was selected arbitrarily.",
            record,
        )
        return save_compiled_release(releases_compiled[0], collection, ocid)

    elif len(releases_compiled) == 1:
        # There is just one compiled release - pass it through unchanged, and log that.
        logger.warning(
            "This record %s already has multiple compiled releases in the releases array!"
            "It was passed through this transform unchanged.",
            record,
        )

        return save_compiled_release(releases_compiled[0], collection, ocid)

    else:
        # We can't process this ocid. Warn of that.
        logger.warning(
            "Record %s could not be compiled because at least one release in the releases array is a linked release "
            "or there are no releases with dates, and the record has neither a compileRelease nor a release with a "
            "tag of 'compiled'.",
            record,
        )

    return None
