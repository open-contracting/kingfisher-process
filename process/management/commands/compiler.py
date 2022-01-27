import logging

from django.core.management.base import BaseCommand
from django.db import transaction
from yapw.methods.blocking import ack, publish

from process.models import Collection, CollectionFile, ProcessingStep, Record, Release
from process.util import consume, create_step, decorator

consume_routing_keys = ["file_worker", "collection_closed"]
routing_key = "compiler"
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        consume(callback, routing_key, consume_routing_keys, decorator=decorator, prefetch_count=20)


def callback(client_state, channel, method, properties, input_message):
    collection = None
    collection_file = None

    # received message from collection closed api endpoint
    if "source" in input_message and input_message["source"] == "collection_closed":
        collection = Collection.objects.get(pk=input_message["collection_id"])
    # received message from regular file processing
    else:
        collection_file = CollectionFile.objects.select_related("collection").get(
            pk=input_message["collection_file_id"]
        )
        collection = collection_file.collection

    ack(client_state, channel, method.delivery_tag)

    if compilable(collection.pk):
        logger.debug("Collection %s is compilable.", collection)

        if collection.data_type and collection.data_type["format"] == Collection.DataTypes.RELEASE_PACKAGE:
            real_files_count = CollectionFile.objects.filter(collection=collection).count()
            if collection.expected_files_count and collection.expected_files_count <= real_files_count:
                # plans compilation of the whole collection (everything is stored yet)
                publish_releases(client_state, channel, collection)
            else:
                logger.debug(
                    "Collection %s is not compilable yet. There are (probably) some"
                    "unprocessed messages in the queue with the new items"
                    " - expected files count %s real files count %s",
                    collection,
                    collection.expected_files_count,
                    real_files_count,
                )

        if (
            collection_file
            and collection.data_type
            and collection.data_type["format"] == Collection.DataTypes.RECORD_PACKAGE
        ):
            # plans compilation of this file (immedaite compilation - we dont have to wait for all records)
            publish_records(client_state, channel, collection_file)
        else:
            logger.debug(
                """
                    There is no collection_file avalable for %s,
                    Message probably comming from api endpoint collection_closed.
                    This log entry can be ignored for collections with record_packages.
                """,
                collection,
            )
    else:
        logger.debug("Collection %s is not compilable.", collection)


def publish_releases(client_state, channel, collection):
    with transaction.atomic():
        compiled_collection = (
            Collection.objects.select_for_update()
            .filter(transform_type=Collection.Transforms.COMPILE_RELEASES)
            .filter(compilation_started=False)
            .get(parent=collection)
        )

        compiled_collection.compilation_started = True
        compiled_collection.save()

    ocids = Release.objects.filter(collection=collection).order_by().values("ocid").distinct()

    for item in ocids:
        create_step(ProcessingStep.Types.COMPILE, compiled_collection.pk, ocid=item["ocid"])

        message = {
            "ocid": item["ocid"],
            "collection_id": collection.pk,
            "compiled_collection_id": compiled_collection.pk,
        }
        publish(client_state, channel, message, "compiler_release")


def publish_records(client_state, channel, collection_file):
    with transaction.atomic():
        compiled_collection = (
            Collection.objects.select_for_update()
            .filter(transform_type=Collection.Transforms.COMPILE_RELEASES)
            .get(parent_id=collection_file.collection.pk)
        )

        if not compiled_collection.compilation_started:
            compiled_collection.compilation_started = True
            compiled_collection.save()

    ocids = (
        Record.objects.filter(collection_file_item__collection_file=collection_file)
        .order_by()
        .values("ocid")
        .distinct()
    )

    for item in ocids:
        create_step(ProcessingStep.Types.COMPILE, compiled_collection.pk, ocid=item["ocid"])

        message = {
            "ocid": item["ocid"],
            "collection_id": collection_file.collection.pk,
            "compiled_collection_id": compiled_collection.pk,
        }
        publish(client_state, channel, message, "compiler_record")


def compilable(collection_id):
    """
    Checks whether the collection
        * should be compiled (compile in steps)
        * could be compiled (load complete)
        * already wasn't compiled

    :param int collection_id: collection id - to be checked

    :returns: true if the collection can be created
    :rtype: bool
    """

    collection = Collection.objects.get(pk=collection_id)

    if not collection.steps or "compile" not in collection.steps:
        logger.debug("Collection %s not compilable (step missing)", collection)
        return False

    # records can be processed immediately
    if collection.data_type and collection.data_type["format"] == Collection.DataTypes.RECORD_PACKAGE:
        return True

    if collection.store_end_at is None:
        logger.debug("Collection %s not compilable (store_end_at not set)", collection)
        return False

    has_remaining_steps = (
        ProcessingStep.objects.filter(collection=collection.get_root_parent())
        .filter(name=ProcessingStep.Types.LOAD)
        .exists()
    )
    if has_remaining_steps:
        logger.debug("Collection %s not compilable (load steps remaining)", collection)
        return False

    compiled_collection = collection.get_compiled_collection()
    if compiled_collection.compilation_started:
        logger.debug("Collection %s not compilable (already started)", collection)
        return False

    return True
