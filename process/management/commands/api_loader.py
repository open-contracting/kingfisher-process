import os.path

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils.translation import gettext as t
from yapw.methods import ack, publish

from process.models import Collection
from process.processors.loader import create_collection_file
from process.util import consume, decorator
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
    collection_file_url = input_message["url"]

    collection = Collection.objects.get(pk=collection_id)
    if collection.deleted_at:
        ack(client_state, channel, method.delivery_tag)
        return

    # In Kingfisher Collect, `path` is set if and only if `errors` isn't set.
    if "path" in input_message:
        filename = os.path.join(settings.KINGFISHER_COLLECT_FILES_STORE, input_message["path"])
    else:
        filename = collection_file_url

    with transaction.atomic():
        collection_file = create_collection_file(
            collection, filename=filename, url=collection_file_url, errors=input_message.get("errors")
        )

    # FileError items from Kingfisher Collect are not processed further.
    if "errors" not in input_message:
        message = {"collection_id": collection_id, "collection_file_id": collection_file.pk}
        publish(client_state, channel, message, routing_key)

    ack(client_state, channel, method.delivery_tag)
