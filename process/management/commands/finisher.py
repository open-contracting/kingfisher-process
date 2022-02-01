import logging

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models.functions import Now
from yapw.methods.blocking import ack

from process.models import Collection
from process.util import consume, decorator

# Read all messages that might be the final message. "file_worker" can be the final message if neither checking nor
# compiling are performed, and if the "collection_closed" message is processed before the "file_worker" message.
consume_routing_keys = ["file_worker", "checker", "release_compiler", "record_compiler", "collection_closed"]
routing_key = "finisher"
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    The worker is responsible for the final steps in collection processing.
    All the checks and calculations are done at this moment, collection is fully procesed.
    Practically, only the completed status is set on collection.
    """

    def handle(self, *args, **options):
        consume(
            callback,
            routing_key,
            consume_routing_keys,
            decorator=decorator,
        )


def callback(client_state, channel, method, properties, input_message):
    collection_id = input_message["collection_id"]

    with transaction.atomic():
        collection = Collection.objects.select_for_update().get(pk=collection_id)
        if completable(collection):
            if collection.transform_type == Collection.Transforms.COMPILE_RELEASES:
                collection.store_end_at = Now()
            collection.completed_at = Now()
            collection.save()

            upgraded_collection = collection.get_upgraded_collection()
            if upgraded_collection:
                upgraded_collection.completed_at = Now()
                upgraded_collection.save()

    ack(client_state, channel, method.delivery_tag)


def completable(collection):
    if collection.completed_at:
        logger.warning("Collection %s not completable (already completed)", collection)
        return False

    # The compiler worker changes `compilation_started` to `True`, then creates the processing steps. This check is
    # required, to avoid a false positive from the `has_steps_remaining` check, below.
    if collection.transform_type == Collection.Transforms.COMPILE_RELEASES and not collection.compilation_started:
        logger.debug("Collection %s not completable (compile steps not created)", collection)
        return False

    # The close_collection endpoint, load command and close command set `store_end_at` for the original and upgraded
    # collections. (Upgrading is performed at the same time as loading.)
    #
    # The finisher worker sets `store_end_at` for the compiled collection, Loading for a compile-releases collection
    # is synonymous with compiling, which is performed in the previous step.
    if collection.store_end_at is None and (
        collection.transform_type != Collection.Transforms.COMPILE_RELEASES
        or collection.get_root_parent().store_end_at is None
    ):
        logger.debug("Collection %s not completable (load incomplete)", collection)
        return False

    has_steps_remaining = collection.processing_steps.exists()
    if has_steps_remaining:
        logger.debug("Collection %s not completable (steps remaining)", collection)
        return False

    actual_files_count = collection.collectionfile_set.count()
    if collection.expected_files_count and collection.expected_files_count > actual_files_count:
        logger.debug(
            "Collection %s not completable. There are (probably) some unprocessed messages in the queue with the new "
            "items - expected files count %s, real files count %s",
            collection,
            collection.expected_files_count,
            actual_files_count,
        )
        return False

    return True
