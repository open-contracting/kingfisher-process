import logging

from django.conf import settings
from django.core.management.base import BaseCommand
from yapw.methods.blocking import ack, publish

from process.models import Collection, CollectionFile, ProcessingStep, Record
from process.util import consume, create_step, decorator

consume_routing_keys = ["file_worker", "collection_closed"]
routing_key = "compiler"
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        consume(callback, routing_key, consume_routing_keys, decorator=decorator)


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

    if compilable(collection):
        if collection.data_type["format"] == Collection.DataTypes.RELEASE_PACKAGE:
            publish_releases(client_state, channel, collection)
        elif collection.data_type["format"] == Collection.DataTypes.RECORD_PACKAGE and collection_file:
            publish_records(client_state, channel, collection_file)


def _set_compilation_started(parent):
    collection = Collection.objects.get(parent=parent, transform_type=Collection.Transform.COMPILE_RELEASES)

    # Use optimistic locking to update the collection.
    updated = Collection.objects.filter(pk=collection.pk, compilation_started=False).update(compilation_started=True)

    return collection, updated


def _publish(client_state, channel, collection, compiled_collection, items, routing_key):
    for item in items.order_by().values("ocid").distinct():
        create_step(ProcessingStep.Name.COMPILE, compiled_collection.pk, ocid=item["ocid"])

        message = {
            "ocid": item["ocid"],
            "collection_id": collection.pk,
            "compiled_collection_id": compiled_collection.pk,
        }
        publish(client_state, channel, message, routing_key)


def publish_releases(client_state, channel, collection):
    compiled_collection, updated = _set_compilation_started(collection)

    # Return if another compiler worker received a message for the same compilable collection.
    if not updated:
        return

    items = collection.release_set
    _publish(client_state, channel, collection, compiled_collection, items, "compiler_release")


def publish_records(client_state, channel, collection_file):
    compiled_collection, updated = _set_compilation_started(collection_file.collection)

    items = Record.objects.filter(collection_file_item__collection_file=collection_file)
    _publish(client_state, channel, collection_file.collection, compiled_collection, items, "compiler_record")


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
    if collection.data_type["format"] == Collection.DataTypes.RECORD_PACKAGE:
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

    # At this point, we know that collection.data_type["format"] == Collection.DataTypes.RELEASE_PACKAGE.
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
