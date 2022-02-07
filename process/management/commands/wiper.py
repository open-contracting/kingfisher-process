import logging

from django.core.management.base import BaseCommand
from yapw.methods.blocking import ack, nack

from process.models import Collection
from process.util import consume, decorator

consume_routing_keys = ["wiper"]
routing_key = "wiper"

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        consume(callback, routing_key, consume_routing_keys, decorator=decorator)


def callback(client_state, channel, method, properties, input_message):
    collection_id = input_message["collection_id"]

    try:
        Collection.objects.get(pk=collection_id).delete()
    except Collection.DoesNotExist as exception:
        logger.error(f"{exception.__class__.__name__} possibly caused by duplicate message: {exception}")
        nack(client_state, channel, method.delivery_tag, requeue=False)
    else:
        ack(client_state, channel, method.delivery_tag)
