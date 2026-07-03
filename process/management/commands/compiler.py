import functools
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
    help = w(t("Start compilation and route messages to the release compiler or record compiler"))

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

    data_type = collection.data_type

    # Acknowledge and return if there's no action to perform.
    if (
        # The collection is cancelled or deleted.
        collection.deleted_at
        # A "collection_closed" message for a "record package" collection.
        or (data_type and data_type["format"] == Format.record_package and not collection_file)
        # The collection isn't compilable.
        or not compilable(collection)
    ):
        ack(client_state, channel, method.delivery_tag)
        return

    compiled_collection = collection.get_compiled_collection()  # PERF: Already called in compilable()

    # Acknowledge and return if there is no compiled collection.
    if compiled_collection is None:
        ack(client_state, channel, method.delivery_tag)
        return

    # Claim the collection with optimistic locking, to prevent concurrent processing.
    updated = Collection.objects.filter(pk=compiled_collection.pk, compilation_started=False).update(
        compilation_started=True
    )

    # Acknowledge and return if the collection expected no files.
    if _collection_is_empty(collection):  # PERF: Already called in compilable()
        ack(client_state, channel, method.delivery_tag)
        return

    data_format = data_type["format"]

    match data_format:
        case Format.record_package:
            items = Record.objects.filter(collection_file=collection_file)
            publish_routing_key = "compiler_record"
        case Format.release_package:
            # If another message already claimed this collection, and this message is not its redelivery.
            if not updated and not method.redelivered:
                ack(client_state, channel, method.delivery_tag)
                return

            # In case this callback is interrupted and the message is redelivered, this together with order_by()
            # guarantees identical batches. See compile_release_batch() for details.
            items = collection.release_set
            publish_routing_key = "compiler_release"
        case Format.compiled_release:
            # Should only occur if setting the --compile option when using the load command with compiled releases.
            ack(client_state, channel, method.delivery_tag)
            return

    publish_compile = functools.partial(
        _publish, client_state, channel, collection, compiled_collection, publish_routing_key
    )

    batch = []
    for ocid in items.values_list("ocid", flat=True).distinct().order_by("ocid").iterator():
        create_step(ProcessingStep.Name.COMPILE, compiled_collection.pk, ocid=ocid)

        if data_format == Format.release_package:
            # Batch OCIDs for a "release package" collection.
            batch.append(ocid)
            if len(batch) >= settings.COMPILE_BATCH_SIZE:
                publish_compile(ocids=batch)
                batch = []
        else:
            publish_compile(ocid=ocid)

    if batch:
        publish_compile(ocids=batch)

    if data_format == Format.release_package:
        # Without this, if the release_compiler worker processes all existing steps while new steps are being created,
        # then the finisher worker can complete the collection prematurely.
        Collection.objects.filter(pk=compiled_collection.pk).update(compilation_enqueued=True)
    elif collection_file:  # data_format is Format.record_package, since Format.compiled_release returns early
        # For "record package" collections, track compilation per file, to avoid a race condition where:
        #
        # - compiler sets compilation_started on the collection (above).
        # - file_worker deletes the last LOAD step and publishes a message, consumed by compiler and finisher.
        # - finisher finds no processing steps and completes the *original* collection.
        # - record_compiler deletes the last COMPILE step and publishes a message, consumed by finisher.
        # - finisher finds no processing steps and completes the *compiled* collection. (!)
        # - However, there are messages from file_worker in the queue, from which compiler will create COMPILE steps.
        collection_file.compilation_started = True
        collection_file.save(update_fields=["compilation_started"])

    # Acknowledge only after all steps and messages are created, to not leave any OCIDs permanently uncompiled.
    ack(client_state, channel, method.delivery_tag)


def _publish(client_state, channel, collection, compiled_collection, routing_key, **payload):
    message = {"collection_id": collection.pk, "compiled_collection_id": compiled_collection.pk, **payload}
    publish(client_state, channel, message, routing_key)


def compilable(collection):
    # 1. Check whether compilation *should* occur.

    # This also matches when collection.transform_type == Collection.Transform.COMPILE_RELEASES.
    if "compile" not in collection.steps:
        logger.debug("Collection %s not compilable ('compile' step not set)", collection)
        return False

    # 2. Check whether compilation *can* occur.

    if _collection_is_empty(collection):
        return True

    # This can occur if the close endpoint is called before the file_worker worker can process any messages.
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
    if compiled_collection and compiled_collection.compilation_enqueued:
        logger.debug("Collection %s not compilable (compile steps already created)", collection)
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
            "Collection %s not compilable. There may be queued messages for the remaining files - "
            "expected files count %s, real files count %s",
            collection,
            collection.expected_files_count,
            actual_files_count,
        )
        return False

    return True


def _collection_is_empty(collection):
    # Note: expected_files_count is None if the close endpoint hasn't been called (e.g. using load command).
    is_empty = collection.expected_files_count == 0

    if is_empty and (count := collection.collectionfile_set.count()):
        # Only reachable if the close request incorrectly omits the file count (which then gets set to 0).
        logger.error("Collection %s expected 0 files but has %s files, compiling", collection, count)
        return False

    return is_empty
