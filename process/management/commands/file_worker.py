import logging
import traceback

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import IntegrityError, transaction
from yapw.methods.blocking import ack, nack, publish

from process.models import Collection, CollectionNote, ProcessingStep
from process.processors.file_loader import process_file
from process.util import clean_thread_resources, create_client, create_step, delete_step, save_note

consume_routing_keys = ["loader", "api_loader"]
routing_key = "file_worker"
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        create_client(prefetch_count=20).consume(callback, routing_key, consume_routing_keys)


def callback(client_state, channel, method, properties, input_message):
    collection_id = input_message["collection_id"]
    collection_file_id = input_message["collection_file_id"]

    try:
        upgraded_collection_file_id = None

        message = {
            "collection_id": input_message["collection_id"],
            "collection_file_id": input_message["collection_file_id"],
        }

        with transaction.atomic():
            upgraded_collection_file_id = process_file(collection_file_id)

            delete_step(ProcessingStep.Types.LOAD, collection_file_id=collection_file_id)

        if settings.ENABLE_CHECKER:
            create_step(ProcessingStep.Types.CHECK, collection_id, collection_file_id=collection_file_id)

        publish(client_state, channel, message, routing_key)

        # send upgraded collection file to further processing
        if upgraded_collection_file_id:
            message["collection_file_id"] = upgraded_collection_file_id

            if settings.ENABLE_CHECKER:
                create_step(ProcessingStep.Types.CHECK, collection_id, collection_file_id=upgraded_collection_file_id)

            publish(client_state, channel, message, routing_key)

        ack(client_state, channel, method.delivery_tag)
    except IntegrityError:
        logger.exception(
            "This should be a very rare exception, most probably one worker stored data item during processing "
            "the very same data in current worker. Message: %s",
            input_message,
        )

        # return message to queue
        nack(client_state, channel, method.delivery_tag)
    except Exception:
        logger.exception("Something went wrong when processing %s", input_message)
        try:
            collection = Collection.objects.get(collectionfile__pk=collection_file_id)
            save_note(
                collection,
                CollectionNote.Codes.ERROR,
                f"Unable to process collection_file_id {collection_file_id}\n{traceback.format_exc()}",
            )
        except Exception:
            logger.exception("Failed saving collection note")

        delete_step(ProcessingStep.Types.LOAD, collection_file_id=collection_file_id)

        ack(client_state, channel, method.delivery_tag)

    clean_thread_resources()
