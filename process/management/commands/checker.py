import logging
import traceback

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from yapw.methods.blocking import ack, publish

from process.exceptions import AlreadyExists
from process.models import Collection, CollectionFile, CollectionNote, ProcessingStep
from process.processors.checker import check_collection_file
from process.util import clean_thread_resources, create_client, delete_step, save_note

consume_routing_keys = ["file_worker"]
routing_key = "checker"
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        if not settings.ENABLE_CHECKER:
            raise CommandError("Refusing to start as checker is disabled in settings - see ENABLE_CHECKER value.")

        create_client(prefetch_count=20).consume(callback, routing_key, consume_routing_keys)


def callback(client_state, channel, method, properties, input_message):
    try:
        collection_file = CollectionFile.objects.select_related("collection").get(
            pk=input_message["collection_file_id"]
        )

        if "check" in collection_file.collection.steps:
            try:
                with transaction.atomic():
                    check_collection_file(collection_file)
            except AlreadyExists:
                logger.exception("Checks already calculated for collection file %s", collection_file)
                save_note(
                    collection_file.collection,
                    CollectionNote.Codes.WARNING,
                    "Checks already calculated for collection file {}".format(collection_file),
                )

            logger.info("Checks calculated for collection file %s", collection_file)
        else:
            logger.info("Collection file %s is not checkable. Skip.", collection_file)

        delete_step(ProcessingStep.Types.CHECK, collection_file_id=collection_file.pk)

        message = {
            "collection_file": collection_file.pk,
            "collection_id": collection_file.collection.pk,
        }

        publish(client_state, channel, message, routing_key)
    except Exception:
        logger.exception("Something went wrong when processing %s", input_message)
        try:
            collection = Collection.objects.get(collectionfile__pk=input_message["collection_file_id"])
            save_note(
                collection,
                CollectionNote.Codes.ERROR,
                "Unable to process collection file id {} \n{}".format(
                    input_message["collection_file_id"], traceback.format_exc()
                ),
            )
        except Exception:
            logger.exception("Failed saving collection note")

    ack(client_state, channel, method.delivery_tag)
    clean_thread_resources()
