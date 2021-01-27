import json
import sys

from django.db import transaction

from process.exceptions import AlreadyExists
from process.management.commands.base.worker import BaseWorker
from process.models import CollectionFile
from process.processors.checker import check_collection_file


class Command(BaseWorker):

    worker_name = "checker"

    consume_keys = ["file_worker"]

    def __init__(self):
        super().__init__(self.worker_name)

    def process(self, channel, method, properties, body):
        try:
            # parse input message
            input_message = json.loads(body.decode("utf8"))

            self.debug("Received message {}".format(input_message))

            collection_file = CollectionFile.objects.select_related("collection").get(
                pk=input_message["collection_file_id"]
            )

            try:
                with transaction.atomic():
                    check_collection_file(collection_file)
            except AlreadyExists:
                self.exception("Checks already calculated for collection file {}".format(collection_file))

            self.info("Checks calculated for collection file {}".format(collection_file))
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            self.exception("Something went wrong when processing {}".format(body))
            sys.exit()
