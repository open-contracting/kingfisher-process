import os.path

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models.functions import Now
from yapw.methods.blocking import ack, publish

from process.models import Collection
from process.processors.loader import create_collection_file
from process.util import consume, decorator

consume_routing_keys = ["api"]
routing_key = "api_loader"


class Command(BaseCommand):
    def handle(self, *args, **options):
        consume(callback, routing_key, consume_routing_keys, decorator=decorator, prefetch_count=20)


@transaction.atomic
def callback(client_state, channel, method, properties, input_message):
    collection_id = input_message["collection_id"]
    if input_message.get("errors") and not input_message.get("path"):
        input_message["path"] = input_message.get("url")
    else:
        input_message["path"] = os.path.join(settings.KINGFISHER_COLLECT_FILES_STORE, input_message["path"])

    collection = Collection.objects.get(pk=collection_id)
    collection_file = create_collection_file(
        collection,
        file_path=input_message.get("path"),
        url=input_message.get("url"),
        errors=input_message.get("errors"),
    )

    if input_message.get("close"):
        collection = Collection.objects.select_for_update().get(pk=collection_id)
        collection.store_end_at = Now()
        collection.save()

        upgraded_collection = collection.get_upgraded_collection()
        if upgraded_collection:
            upgraded_collection.store_end_at = Now()
            upgraded_collection.save()

    # FileError items from Kingfisher Collect are not processed further.
    if "errors" not in input_message:
        message = {"collection_id": collection_id, "collection_file_id": collection_file.pk}
        publish(client_state, channel, message, routing_key)

    ack(client_state, channel, method.delivery_tag)
