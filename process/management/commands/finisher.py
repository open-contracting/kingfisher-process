import logging

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models.functions import Now
from yapw.methods.blocking import ack

from process.models import Collection, CollectionFile, ProcessingStep
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
        if completable(collection_id):
            collection = Collection.objects.select_for_update().get(pk=input_message["collection_id"])
            if collection.transform_type == Collection.Transforms.COMPILE_RELEASES:
                collection.store_end_at = Now()

            collection.completed_at = Now()
            collection.save()

            # complete upgraded collection as well
            upgraded_collection = collection.get_upgraded_collection()
            if upgraded_collection:
                upgraded_collection.completed_at = Now()
                upgraded_collection.save()

            logger.debug("Collection %s finished.", collection_id)

    ack(client_state, channel, method.delivery_tag)


def completable(collection_id):
    """
    Checks whether the collection can be marked as completed.

    :param int collection_id: collection id - to be checked

    :returns: true if the collection processing was completed
    :rtype: bool
    """

    collection = Collection.objects.get(pk=collection_id)

    if collection.completed_at:
        logger.warning("Collection %s not completable (already completed)", collection)
        return False

    # compile-releases collections don't set the `store_end_at` field (?) - instead check the root collection.
    if collection.store_end_at is None and (
        collection.transform_type != Collection.Transforms.COMPILE_RELEASES
        or collection.get_root_parent().store_end_at is None
    ):
        logger.debug("Collection %s not completable (load not finished)", collection)
        return False

    # special case when the collection should be compiled and
    # waits for compilation to be planned
    # in such case, no processing steps will be available yet
    if collection.transform_type == Collection.Transforms.COMPILE_RELEASES and not collection.compilation_started:
        logger.debug("Collection %s not completable (compile not started)", collection)
        return False

    has_steps_remaining = ProcessingStep.objects.filter(collection=collection).exists()
    if has_steps_remaining:
        logger.debug("Collection %s not completable (steps remaining)", collection)
        return False

    real_files_count = CollectionFile.objects.filter(collection=collection).count()
    if collection.expected_files_count and collection.expected_files_count > real_files_count:
        logger.debug(
            "Collection %s not completable. There are (probably) some unprocessed messages in the queue with the new "
            "items - expected files count %s, real files count %s",
            collection,
            collection.expected_files_count,
            real_files_count,
        )
        return False

    return True
