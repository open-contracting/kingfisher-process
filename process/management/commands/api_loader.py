import logging
import os.path

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models.functions import Now
from yapw.methods.blocking import ack, publish

from process.models import Collection
from process.processors.loader import create_collection_file
from process.util import create_client, decorator

consume_routing_keys = ["api"]
routing_key = "api_loader"
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        create_client(prefetch_count=20).consume(callback, routing_key, consume_routing_keys, decorator=decorator)


def callback(client_state, channel, method, properties, input_message):
    if input_message.get("errors", None) and not input_message.get("path", None):
        input_message["path"] = input_message.get("url", None)
    else:
        input_message["path"] = os.path.join(settings.KINGFISHER_COLLECT_FILES_STORE, input_message["path"])

    collection_id = input_message["collection_id"]

    collection = Collection.objects.get(pk=collection_id)
    with transaction.atomic():
        collection_file = create_collection_file(
            collection,
            file_path=input_message.get("path", None),
            url=input_message.get("url", None),
            errors=input_message.get("errors", None),
        )

        message = {"collection_id": collection_id, "collection_file_id": collection_file.pk}

        if input_message.get("close", False):
            # close collections as well
            collection = Collection.objects.select_for_update().get(pk=collection_id)
            collection.store_end_at = Now()
            collection.save()

            upgraded_collection = collection.get_upgraded_collection()
            if upgraded_collection:
                upgraded_collection.store_end_at = Now()
                upgraded_collection.save()

    # only files without errors will be further processed
    if "errors" not in input_message:
        publish(client_state, channel, message, routing_key)
    else:
        logger.info(
            "Collection file %s contains errors %s, not sending to further processing.",
            collection_file,
            input_message.get("errors", None),
        )

    ack(client_state, channel, method.delivery_tag)
