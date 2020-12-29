import json
import sys

from django.db import transaction

from process.management.commands.base.worker import BaseWorker
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

    consume_keys = ["compiler"]

    def __init__(self):
        super().__init__(self.worker_name)

    def process(self, channel, method, properties, body):
        try:
            # parse input message
            input_message = json.loads(body.decode("utf8"))

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

            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            channel.basic_nack(delivery_tag=method.delivery_tag)
            self.exception("Something went wrong when processing {}".format(body))
            sys.exit(1)
