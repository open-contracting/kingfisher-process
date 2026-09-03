from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils.translation import gettext as t
from yapw.methods import ack, publish

from process.processors.loader import create_collection_file
from process.util import consume, decorator, lock_collection
from process.util import wrap as w

# Other applications use this routing key.
consume_routing_keys = ["api"]
routing_key = "api_loader"


class Command(BaseCommand):
    help = w(t("Create collection files"))

    def handle(self, *args, **options):
        consume(
            on_message_callback=callback, queue=routing_key, routing_keys=consume_routing_keys, decorator=decorator
        )


def callback(client_state, channel, method, properties, input_message):
    collection_id = input_message["collection_id"]
    url = input_message["url"]
    path = input_message["path"]

    with transaction.atomic():
        # The collection can be cancelled or fully deleted while its messages are queued.
        collection = lock_collection(collection_id)
        if collection is None or collection.deleted_at:
            ack(client_state, channel, method.delivery_tag)
            return

        filename = Path(settings.KINGFISHER_COLLECT_FILES_STORE) / path
        collection_file = create_collection_file(collection, filename=filename, url=url)

    message = {"collection_id": collection_id, "collection_file_id": collection_file.pk}
    publish(client_state, channel, message, routing_key)

    ack(client_state, channel, method.delivery_tag)
