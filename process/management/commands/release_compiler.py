import json
import traceback

from django.db import transaction

from process.management.commands.base.worker import BaseWorker
from process.models import Collection, CollectionNote, ProcessingStep
from process.processors.compiler import compile_release


class Command(BaseWorker):
    """
    The worker is responsible for the compilation of particular releases.
    Consumes messages with an ocid and collection_id which should be compiled.
    The whole structure of CollectionFile, CollectionFileItem, and CompiledRelease
    is created and saved.
    It's safe to run multiple workers of this type at the same type.
    """

    worker_name = "release_compiler"
    consume_keys = ["compiler_release"]
    prefetch_count = 20

    def __init__(self):
        super().__init__(self.worker_name)

    def process(self, connection, channel, delivery_tag, body):
        input_message = json.loads(body.decode("utf8"))

        release_id = None

        try:
            self._debug("Received message %s", input_message)

            ocid = input_message["ocid"]
            collection_id = input_message["collection_id"]
            compiled_collection_id = input_message["compiled_collection_id"]

            with transaction.atomic():
                self._info("Compiling release collection_id: %s ocid: %s", collection_id, ocid)
                release = compile_release(collection_id, ocid)

                if release:
                    release_id = release.pk
        except Exception:
            self._exception("Something went wrong when processing %s", body)
            try:
                collection = Collection.objects.get(id=input_message["collection_id"])
                self._save_note(
                    collection,
                    CollectionNote.Codes.ERROR,
                    "Unable to process {} for collection id : {} \n{}".format(
                        input_message["ocid"], input_message["collection_id"], traceback.format_exc()
                    ),
                )
            except Exception:
                self._exception("Failed saving collection note")

        self._deleteStep(ProcessingStep.Types.COMPILE, collection_id=compiled_collection_id, ocid=ocid)

        # publish message about processed item
        message = {
            "ocid": ocid,
            "compiled_release_id": release_id,
            "collection_id": compiled_collection_id,
        }

        self._publish_async(connection, channel, json.dumps(message))

        self._ack(connection, channel, delivery_tag)
        self._clean_thread_resources()
