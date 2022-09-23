import logging
import types

from django.core.management.base import BaseCommand
from django.db import connections
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
        consume(callback, routing_key, consume_routing_keys, decorator=decorator)


def callback(client_state, channel, method, properties, input_message):
    collection_id = input_message["collection_id"]

    # Django implements "ON DELETE CASCADE" in Python, not in the database. This causes "InternalError: invalid memory
    # alloc request size 1073741824" (1GB) [1]. There is an open issue to implement it in the database [2].
    #
    # 1. https://code.djangoproject.com/ticket/30533
    # 2. https://code.djangoproject.com/ticket/21961
    connections["default"].ops.bulk_batch_size = types.MethodType(bulk_batch_size, connections["default"].ops)

    Collection.objects.get(pk=collection_id).delete()

    ack(client_state, channel, method.delivery_tag)
