from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from yapw.methods.blocking import ack, publish

from process.models import ProcessingStep
from process.processors.file_loader import process_file
from process.util import create_client, create_step, decorator, delete_step

consume_routing_keys = ["loader", "api_loader"]
routing_key = "file_worker"


class Command(BaseCommand):
    def handle(self, *args, **options):
        create_client(prefetch_count=20).consume(callback, routing_key, consume_routing_keys, decorator=decorator)


def callback(client_state, channel, method, properties, input_message):
    collection_id = input_message["collection_id"]
    collection_file_id = input_message["collection_file_id"]

    with transaction.atomic():
        upgraded_collection_file_id = process_file(collection_file_id)
        delete_step(ProcessingStep.Types.LOAD, collection_file_id=collection_file_id)

    if settings.ENABLE_CHECKER:
        create_step(ProcessingStep.Types.CHECK, collection_id, collection_file_id=collection_file_id)

    message = {
        "collection_id": input_message["collection_id"],
        "collection_file_id": input_message["collection_file_id"],
    }
    publish(client_state, channel, message, routing_key)

    # send upgraded collection file to further processing
    if upgraded_collection_file_id:
        if settings.ENABLE_CHECKER:
            create_step(ProcessingStep.Types.CHECK, collection_id, collection_file_id=upgraded_collection_file_id)

        message["collection_file_id"] = upgraded_collection_file_id
        publish(client_state, channel, message, routing_key)

    ack(client_state, channel, method.delivery_tag)
