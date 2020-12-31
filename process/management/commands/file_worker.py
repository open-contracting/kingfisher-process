import json
import sys

from django.db import transaction

from process.management.commands.base.worker import BaseWorker
from process.processors.file_loader import process_file
from process.util import json_dumps


class Command(BaseWorker):

    worker_name = "file_worker"

    consume_keys = ["loader"]

    def __init__(self):
        super().__init__(self.worker_name)

    def process(self, channel, method, properties, body):
        try:
            # parse input message
            input_message = json.loads(body.decode("utf8"))
            self.debug("Received message {}".format(input_message))

            collection_file_id = input_message["collection_file_id"]

            upgraded_collection_file_id = None
            with transaction.atomic():
                upgraded_collection_file_id = process_file(collection_file_id)

                self.deleteStep(collection_file_id)

            self.publish(json.dumps(input_message))

            # send upgraded collection file to further processing
            if upgraded_collection_file_id:
                message = {"collection_file_id": upgraded_collection_file_id}
                self.publish(json_dumps(message))

            # confirm message processing
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            self.exception("Something went wrong when processing {}".format(body))
            sys.exit()
