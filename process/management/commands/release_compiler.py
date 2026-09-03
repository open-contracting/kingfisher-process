import logging

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils.translation import gettext as t
from yapw.methods import ack, publish

from process.models import ProcessingStep
from process.processors.compiler import compile_release_batch
from process.util import consume, decorator, lock_collection
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
    ocids = input_message["ocids"]
    compiled_collection_id = input_message["compiled_collection_id"]

    # Create the compiled releases and delete the COMPILE steps in the same transaction. On IntegrityError, the
    # transaction rolls back. Since the compiler worker guarantees identical batches across redelivered messages,
    # the committing transaction deletes those same steps. No steps are orphaned.
    #
    # Batch-work prevents a deleting_step()-like approach, without extra complexity.
    with transaction.atomic():
        # The compiled collection can be cancelled or fully deleted while its messages are queued.
        compiled_collection = lock_collection(compiled_collection_id)
        if compiled_collection is None or compiled_collection.deleted_at:
            ack(client_state, channel, method.delivery_tag)
            return

        compile_release_batch(compiled_collection, ocids)
        ProcessingStep.objects.filter(
            name=ProcessingStep.Name.COMPILE, collection_id=compiled_collection_id, ocid__in=ocids
        ).delete()

    publish(client_state, channel, {"collection_id": compiled_collection_id}, routing_key)

    ack(client_state, channel, method.delivery_tag)
