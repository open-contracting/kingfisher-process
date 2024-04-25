import logging

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils.translation import gettext as t
from ocdskit.util import Format
from yapw.methods import ack, publish

from process.models import Collection, CollectionFile, ProcessingStep, Record
from process.util import consume, create_step, decorator
from process.util import wrap as w

consume_routing_keys = ["file_worker", "collection_closed"]
routing_key = "compiler"
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = w(t("Start compilation and route messages to the record or release compilers"))

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

    # Acknowledge early when using the Splitter pattern.
    ack(client_state, channel, method.delivery_tag)

    # No action is performed for "collection_closed" messages for "record package" collections.
    if collection.data_type and collection.data_type["format"] == Format.record_package and not collection_file:
        return

    # There is already a guard in the file_worker worker's process_file() function to halt on non-packages, so we only
    # test the "format" to decide the logic, not to decide whether to proceed.
    if compilable(collection):
        compiled_collection = collection.get_compiled_collection()

        # Use optimistic locking to update the collection. (Here, it's the return value that's important.)
        updated = Collection.objects.filter(pk=compiled_collection.pk, compilation_started=False).update(
            compilation_started=True
        )

        # Return if the collection expected no files.
        if _collection_is_empty(collection):
            return

        match collection.data_type["format"]:
            case Format.record_package:
                items = Record.objects.filter(collection_file_item__collection_file=collection_file)
                publish_routing_key = "compiler_record"
            case Format.release_package:
                # Return if another compiler worker received a message for the same compilable collection.
                if not updated:
                    return

                items = collection.release_set
                publish_routing_key = "compiler_release"

        for item in items.values("ocid").distinct().iterator():
            create_step(ProcessingStep.Name.COMPILE, compiled_collection.pk, ocid=item["ocid"])

            message = {
                "ocid": item["ocid"],
                "collection_id": collection.pk,
                "compiled_collection_id": compiled_collection.pk,
            }
            publish(client_state, channel, message, publish_routing_key)


def compilable(collection):
    # 1. Check whether compilation *should* occur.

    # This also matches when collection.transform_type == Collection.Transform.COMPILE_RELEASES.
    if "compile" not in collection.steps:
        logger.debug("Collection %s not compilable ('compile' step not set)", collection)
        return False

    # 2. Check whether compilation *can* occur.

    if _collection_is_empty(collection):
        return True

    # This can occur if the close_collection endpoint is called before the file_worker worker can process any messages.
    if not collection.data_type:
        logger.debug("Collection %s not compilable (data_type not set)", collection)
        return False

    # Records can be compiled immediately without waiting for a complete load.
    if collection.data_type["format"] == Format.record_package:
        return True

    # Run after collection.data_type["format"] == Format.record_package, because records can be compiled immediately.
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

    # At this point, we know that collection.data_type["format"] == Format.release_package.
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


def _collection_is_empty(collection):
    # Note: expected_files_count is None if the close_collection endpoint hasn't been called (e.g. using load command).
    is_empty = collection.expected_files_count == 0
    if is_empty:
        count = collection.collectionfile_set.count()
        assert count == 0, f"{count} is not 0"
    return is_empty
