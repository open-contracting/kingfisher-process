import logging

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils.translation import gettext as t
from yapw.methods import ack, publish

from process.models import Collection, ProcessingStep
from process.processors.compiler import compile_release_batch
from process.util import consume, decorator
from process.util import wrap as w

consume_routing_keys = ["compiler_release"]
routing_key = "release_compiler"
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = w(t("Create compiled releases from releases with the same OCID"))

    def handle(self, *args, **options):
        consume(
            on_message_callback=callback, queue=routing_key, routing_keys=consume_routing_keys, decorator=decorator
        )


def callback(client_state, channel, method, properties, input_message):
    # TEMPORARY: Accept either a batch of OCIDs (new format) or a single OCID (old format).
    ocids = input_message.get("ocids") or [input_message["ocid"]]
    compiled_collection_id = input_message["compiled_collection_id"]

    compiled_collection = Collection.objects.get(pk=compiled_collection_id)
    if compiled_collection.deleted_at:
        ack(client_state, channel, method.delivery_tag)
        return

    with transaction.atomic():
        compile_release_batch(compiled_collection, ocids)
        ProcessingStep.objects.filter(
            name=ProcessingStep.Name.COMPILE, collection_id=compiled_collection_id, ocid__in=ocids
        ).delete()

    publish(client_state, channel, {"collection_id": compiled_collection_id}, routing_key)

    ack(client_state, channel, method.delivery_tag)
