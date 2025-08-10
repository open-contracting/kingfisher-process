import logging
import threading
import time

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models.functions import Now
from django.utils.translation import gettext as t
from yapw.methods import ack, nack

from process.models import Collection
from process.util import consume, decorator
from process.util import wrap as w

# Read all messages that might be the final message. "file_worker" can be the final message if neither checking nor
# compiling are performed, and if the "collection_closed" message is processed before the "file_worker" message.
# Or, in other words, read all messages published by workers that delete steps (since this checks for steps remaining).
consume_routing_keys = [
    "file_worker",
    "checker",
    "release_compiler",
    "record_compiler",
    "collection_closed",
]
routing_key = "finisher"
logger = logging.getLogger(__name__)
lock = threading.Lock()
requeued = set()


class Command(BaseCommand):
    help = w(t("Set collections as completed, close compiled collections and cache row counts"))

    def handle(self, *args, **options):
        consume(
            on_message_callback=callback, queue=routing_key, routing_keys=consume_routing_keys, decorator=decorator
        )


def callback(client_state, channel, method, properties, input_message):
    collection_id = input_message["collection_id"]

    # Run queries only for redelivered messages.
    if method.redelivered:
        collection = Collection.objects.get(pk=collection_id)
        if collection.deleted_at:
            ack(client_state, channel, method.delivery_tag)
            return

        with transaction.atomic():
            if completable(collection):
                # Use optimistic locking to update the collections.
                if collection.transform_type == Collection.Transform.COMPILE_RELEASES:
                    updated = _set_complete_at(collection, store_end_at=Now())
                else:
                    updated = _set_complete_at(collection)

                if updated and (upgraded_collection := collection.get_upgraded_collection()):
                    _set_complete_at(upgraded_collection)

            # If the collection isn't completable or completed, try again after a delay, to prevent churning.
            elif not collection.completed_at:
                # RabbitMQ won't deliver another message until ack'd, so blocking the thread with time.sleep() is
                # effectively the same as not blocking the thread with client_state.connection.ioloop.call_later().
                time.sleep(30)

                # Note: Need to monitor the queue, in case a message gets stuck.
                # Log the collection for administrators to use in the cancelcollection command.
                logger.info("Collection %s requeued", collection)

                nack(client_state, channel, method.delivery_tag, requeue=True)
                return

    # If no message has yet been requeued for the collection, track the collection and requeue the message.
    elif collection_id not in requeued:  # no lock at first, for performance
        # Use a lock and re-do the check, to prevent multiple requeues within each collection.
        # If the collections were known in advance, we could use a non-blocking lock for each collection.
        with lock:
            if collection_id not in requeued:
                requeued.add(collection_id)
                nack(client_state, channel, method.delivery_tag, requeue=True)
                return

    ack(client_state, channel, method.delivery_tag)


def _set_complete_at(collection, **kwargs):
    # all() avoids cached calls in count().
    # https://docs.djangoproject.com/en/4.2/ref/models/querysets/#all
    # https://docs.djangoproject.com/en/4.2/ref/models/querysets/#count
    count = {
        "cached_releases_count": collection.release_set.all().count(),
        "cached_records_count": collection.record_set.all().count(),
        "cached_compiled_releases_count": collection.compiledrelease_set.all().count(),
    }
    return Collection.objects.filter(pk=collection.pk, completed_at=None).update(completed_at=Now(), **count, **kwargs)


def completable(collection):
    if collection.completed_at:
        logger.warning("Collection %s not completable (already completed)", collection)
        return False

    # The compiler worker changes `compilation_started` to `True`, then creates the processing steps. This check is
    # required, to avoid a false positive from the "steps remaining" check, below.
    if collection.transform_type == Collection.Transform.COMPILE_RELEASES and not collection.compilation_started:
        logger.debug("Collection %s not completable (compile steps not created)", collection)
        return False

    # The close_collection endpoint, load command and closecollection command set `store_end_at` for the original and
    # upgraded collections. (Upgrading is performed at the same time as loading.)
    #
    # The finisher worker sets `store_end_at` for the compiled collection, Loading for a "compile-releases" collection
    # is synonymous with compiling, which is performed in the previous step.
    if collection.store_end_at is None and (
        collection.transform_type != Collection.Transform.COMPILE_RELEASES
        or collection.get_root_parent().store_end_at is None
    ):
        logger.debug("Collection %s not completable (load incomplete)", collection)
        return False

    if collection.processing_steps.exists():
        logger.debug("Collection %s not completable (steps remaining)", collection)
        return False

    if collection.expected_files_count:
        actual_files_count = collection.collectionfile_set.count()
        if collection.expected_files_count > actual_files_count:
            logger.debug(
                "Collection %s not completable. There are (probably) some unprocessed messages in the queue with the "
                "new items - expected files count %s, real files count %s",
                collection,
                collection.expected_files_count,
                actual_files_count,
            )
            return False

    return True
