import json
import traceback

from django.db import transaction

from process.management.commands.base.worker import BaseWorker
from process.models import Collection, CollectionNote
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

    def __init__(self):
        super().__init__(self.worker_name)

    def process(self, channel, method, properties, body):
        # parse input message
        input_message = json.loads(body.decode("utf8"))

        try:
            self.debug("Received message {}".format(input_message))

            ocid = input_message["ocid"]
            collection_id = input_message["collection_id"]

            with transaction.atomic():
                self.info("Compiling release collection_id: {} ocid: {}".format(collection_id, ocid))
                release = compile_release(collection_id, ocid)

            # publish message about processed item
            message = {
                "ocid": ocid,
                "compiled_release_id": release.pk,
            }

            self.publish(json.dumps(message))

        except Exception:
            self.exception("Something went wrong when processing {}".format(body))
            try:
                collection = Collection.objects.get(id=input_message["collection_id"])
                self.save_note(
                    collection,
                    CollectionNote.Codes.ERROR,
                    "Unable to process {} for collection id : {} \n{}".format(
                        input_message["ocid"], input_message["collection_id"], traceback.format_exc()
                    ),
                )
            except Exception:
                self.exception("Failed saving collection note")

        channel.basic_ack(delivery_tag=method.delivery_tag)
