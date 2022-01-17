import logging

from django.core.management.base import BaseCommand
from yapw.methods.blocking import ack

from process.models import Collection
from process.util import create_client

consume_routing_keys = ["wiper"]
routing_key = "wiper"
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        create_client().consume(callback, routing_key, consume_routing_keys)


def callback(client_state, channel, method, properties, input_message):
    try:
        collection_id = input_message["collection_id"]

        collection = Collection.objects.get(pk=collection_id)
        logger.debug("Deleting collection %s", collection)

        collection.delete()

        logger.info("Collection %s deleted.", collection)
    except Collection.DoesNotExist:
        logger.error("Collection %d not found", input_message["collection_id"])

    ack(client_state, channel, method.delivery_tag)
