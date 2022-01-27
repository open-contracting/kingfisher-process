from django.core.management.base import BaseCommand
from django.db import transaction
from yapw.methods.blocking import ack

from process.models import Collection
from process.util import consume, decorator

consume_routing_keys = ["wiper"]
routing_key = "wiper"


class Command(BaseCommand):
    def handle(self, *args, **options):
        consume(
            callback,
            routing_key,
            consume_routing_keys,
            decorator=decorator,
        )


@transaction.atomic
def callback(client_state, channel, method, properties, input_message):
    collection_id = input_message["collection_id"]

    Collection.objects.get(pk=collection_id).delete()

    ack(client_state, channel, method.delivery_tag)
