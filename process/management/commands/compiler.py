import logging

from django.core.management.base import BaseCommand
from django.db import transaction
from yapw.methods.blocking import ack, publish

from process.models import Collection, CollectionFile, ProcessingStep, Record, Release
from process.processors.compiler import compilable
from process.util import create_client, create_step, decorator

consume_routing_keys = ["file_worker", "collection_closed"]
routing_key = "compiler"
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        create_client(prefetch_count=20).consume(callback, routing_key, consume_routing_keys, decorator=decorator)


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
    try:
        with transaction.atomic():
            compiled_collection = (
                Collection.objects.select_for_update()
                .filter(transform_type=Collection.Transforms.COMPILE_RELEASES)
                .filter(compilation_started=False)
                .get(parent=collection)
            )

            compiled_collection.compilation_started = True
            compiled_collection.save()

        logger.info("Planning release compilation for %s", compiled_collection)

        # get all ocids for collection
        ocids = Release.objects.filter(collection=collection).order_by().values("ocid").distinct()

        for item in ocids:
            # send message to a next phase
            message = {
                "ocid": item["ocid"],
                "collection_id": collection.pk,
                "compiled_collection_id": compiled_collection.pk,
            }

            create_step(ProcessingStep.Types.COMPILE, compiled_collection.pk, ocid=item["ocid"])
            publish(client_state, channel, message, "compiler_release")
    except Collection.DoesNotExist:
        logger.warning(
            "Tried to plan compilation for already 'planned' collection."
            "This can rarely happen in multi worker environments."
        )


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

    logger.info("Planning records compilation for %s file %s", compiled_collection, collection_file)

    # get all ocids for collection
    ocids = (
        Record.objects.filter(collection_file_item__collection_file=collection_file)
        .order_by()
        .values("ocid")
        .distinct()
    )

    for item in ocids:
        # send message to a next phase
        message = {
            "ocid": item["ocid"],
            "collection_id": collection_file.collection.pk,
            "compiled_collection_id": compiled_collection.pk,
        }

        create_step(ProcessingStep.Types.COMPILE, compiled_collection.pk, ocid=item["ocid"])
        publish(client_state, channel, message, "compiler_record")
