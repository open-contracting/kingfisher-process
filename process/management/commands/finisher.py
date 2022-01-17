import logging
import traceback

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models.functions import Now
from yapw.methods.blocking import ack

from process.models import Collection, CollectionNote
from process.processors.finisher import completable
from process.util import clean_thread_resources, create_client, save_note

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
        create_client().consume(callback, routing_key, consume_routing_keys)


def callback(client_state, channel, method, properties, input_message):
    try:
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

                logger.debug("Processing of collection_id: %s finished. Set as completed.", collection_id)
            else:
                logger.debug("Processing of collection_id: %s not completable", collection_id)

    except Exception:
        logger.exception("Something went wrong when processing %s", input_message)
        try:
            collection = Collection.objects.get(pk=input_message["collection_id"])
            save_note(
                collection,
                CollectionNote.Codes.ERROR,
                "Unable to process message for collection id : {} \n{}".format(
                    input_message["collection_id"], traceback.format_exc()
                ),
            )
        except Exception:
            logger.exception("Failed saving collection note")

    ack(client_state, channel, method.delivery_tag)
    clean_thread_resources()
