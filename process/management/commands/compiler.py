import logging

from django.conf import settings
from django.core.management.base import BaseCommand
from yapw.methods import ack, publish

from process.models import Collection, CollectionFile, ProcessingStep, Record
from process.util import RECORD_PACKAGE, RELEASE_PACKAGE, consume, create_step, decorator

consume_routing_keys = ["file_worker", "collection_closed"]
routing_key = "compiler"
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        consume(
            on_message_callback=callback, queue=routing_key, routing_keys=consume_routing_keys, decorator=decorator
        )


def callback(client_state, channel, method, properties, input_message):
    collection_id = input_message["collection_id"]
    collection_file_id = input_message.get("collection_file_id")  # None if collection_closed

    if method.routing_key == f"{settings.RABBIT_EXCHANGE_NAME}_collection_closed":
        collection = Collection.objects.get(pk=collection_id)
        collection_file = None
    else:
        collection_file = CollectionFile.objects.select_related("collection").get(pk=collection_file_id)
        collection = collection_file.collection

    ack(client_state, channel, method.delivery_tag)

    # No action is performed for "collection_closed" messages for "record package" collections.
    if collection.data_type and collection.data_type["format"] == RECORD_PACKAGE and not collection_file:
        return

    # There is already a guard in the file_worker worker's process_file function to halt on non-packages, so we only
    # test the "format" to decide the logic, not to decide whether to proceed.
    if compilable(collection):
        compiled_collection = collection.get_compiled_collection()

        # Use optimistic locking to update the collection.
        updated = Collection.objects.filter(pk=compiled_collection.pk, compilation_started=False).update(
            compilation_started=True
        )

        if collection.data_type["format"] == RELEASE_PACKAGE:
            # Return if another compiler worker received a message for the same compilable collection.
            if not updated:
                return
            items = collection.release_set
            publish_routing_key = "compiler_release"
        elif collection.data_type["format"] == RECORD_PACKAGE:
            items = Record.objects.filter(collection_file_item__collection_file=collection_file)
            publish_routing_key = "compiler_record"

        for item in items.order_by().values("ocid").distinct():
            create_step(ProcessingStep.Name.COMPILE, compiled_collection.pk, ocid=item["ocid"])

            message = {
                "ocid": item["ocid"],
                "collection_id": collection.pk,
                "compiled_collection_id": compiled_collection.pk,
            }
            publish(client_state, channel, message, publish_routing_key)


def compilable(collection):
    # 1. Check whether compilation *should* occur.

    if not collection.steps or "compile" not in collection.steps:
        logger.debug("Collection %s not compilable ('compile' step not set)", collection)
        return False

    # 2. Check whether compilation *can* occur.

    # This can occur if the close_collection endpoint is called before the file_worker worker can process any messages.
    if not collection.data_type:
        logger.debug("Collection %s not compilable (data_type not set)", collection)
        return False

    # Records can be compiled immediately without waiting for a complete load.
    if collection.data_type["format"] == RECORD_PACKAGE:
        return True

    if collection.store_end_at is None:
        logger.debug("Collection %s not compilable (load incomplete)", collection)
        return False

    # 3. Check whether compilation hasn't started. (2. then continues below, to put slower queries later.)

    compiled_collection = collection.get_compiled_collection()
    if compiled_collection.compilation_started:
        logger.debug("Collection %s not compilable (already started)", collection)
        return False

    has_load_steps_remaining = (
        collection.get_root_parent().processing_steps.filter(name=ProcessingStep.Name.LOAD).exists()
    )
    if has_load_steps_remaining:
        logger.debug("Collection %s not compilable (load steps remaining)", collection)
        return False

    # At this point, we know that collection.data_type["format"] == RELEASE_PACKAGE.
    actual_files_count = collection.collectionfile_set.count()
    if collection.expected_files_count and collection.expected_files_count > actual_files_count:
        logger.debug(
            "Collection %s not compilable. There are (probably) some unprocessed messages in the queue with the "
            "new items - expected files count %s, real files count %s",
            collection,
            collection.expected_files_count,
            actual_files_count,
        )
        return False

    return True
