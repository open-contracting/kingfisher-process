import logging
import traceback

from django.core.management.base import BaseCommand
from django.db import transaction
from yapw.methods.blocking import ack, publish

from process.models import Collection, CollectionNote, ProcessingStep
from process.processors.compiler import compile_release
from process.util import clean_thread_resources, create_client, delete_step, save_note

consume_routing_keys = ["compiler_release"]
routing_key = "release_compiler"
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    The worker is responsible for the compilation of particular releases.
    Consumes messages with an ocid and collection_id which should be compiled.
    The whole structure of CollectionFile, CollectionFileItem, and CompiledRelease
    is created and saved.
    It's safe to run multiple workers of this type at the same type.
    """

    def handle(self, *args, **options):
        create_client(prefetch_count=20).consume(callback, routing_key, consume_routing_keys)


def callback(client_state, channel, method, properties, input_message):
    release_id = None

    try:
        ocid = input_message["ocid"]
        collection_id = input_message["collection_id"]
        compiled_collection_id = input_message["compiled_collection_id"]

        with transaction.atomic():
            logger.info("Compiling release collection_id: %s ocid: %s", collection_id, ocid)
            release = compile_release(collection_id, ocid)

            if release:
                release_id = release.pk
    except Exception:
        logger.exception("Something went wrong when processing %s", input_message)
        try:
            collection = Collection.objects.get(pk=input_message["collection_id"])
            save_note(
                collection,
                CollectionNote.Codes.ERROR,
                "Unable to process {} for collection id : {} \n{}".format(
                    input_message["ocid"], input_message["collection_id"], traceback.format_exc()
                ),
            )
        except Exception:
            logger.exception("Failed saving collection note")

    delete_step(ProcessingStep.Types.COMPILE, collection_id=compiled_collection_id, ocid=ocid)

    # publish message about processed item
    message = {
        "ocid": ocid,
        "compiled_release_id": release_id,
        "collection_id": compiled_collection_id,
    }

    publish(client_state, channel, message, routing_key)

    ack(client_state, channel, method.delivery_tag)
    clean_thread_resources()
