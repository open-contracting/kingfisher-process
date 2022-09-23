import logging

from django.core.management.base import BaseCommand
from django.db import connection
from yapw.methods.blocking import ack

from process.models import Collection
from process.util import consume, decorator

consume_routing_keys = ["wiper"]
routing_key = "wiper"

logger = logging.getLogger(__name__)


def bulk_batch_size(self, fields, objs):
    return 65536  # 2**16


class Command(BaseCommand):
    def handle(self, *args, **options):
        # Django implements "ON DELETE CASCADE" in Python, not in the database. This causes "InternalError: invalid
        # memory alloc request size 1073741824" (1GB) due to the memory required.
        # https://code.djangoproject.com/ticket/30533
        connection.ops.bulk_batch_size = bulk_batch_size

        consume(callback, routing_key, consume_routing_keys, decorator=decorator)


def callback(client_state, channel, method, properties, input_message):
    collection_id = input_message["collection_id"]

    Collection.objects.get(pk=collection_id).delete()

    ack(client_state, channel, method.delivery_tag)
