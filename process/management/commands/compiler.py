import json
import sys

from django.db import transaction

from process.exceptions import AlreadyExists
from process.management.commands.base.worker import BaseWorker
from process.models import CollectionFile, Release
from process.processors.compiler import compilable, create_compiled_collection


class Command(BaseWorker):

    worker_name = "compiler"

    consume_keys = ["file_worker"]

    def __init__(self):
        super().__init__(self.worker_name)

    def process(self, channel, method, properties, body):
        try:
            # parse input message
            input_message = json.loads(body.decode("utf8"))

            self.debug("Received message {}".format(input_message))

            try:
                collection_file = CollectionFile.objects.prefetch_related("collection").get(
                    pk=input_message["collection_file_id"]
                )

                collection = collection_file.collection

                if compilable(collection.id):
                    try:
                        with transaction.atomic():
                            create_compiled_collection(collection.id)

                        self.info("Planning release compilation")

                        # get all ocids for collection
                        ocids = (
                            Release.objects.filter(collection_file_item__collection_file__collection=collection)
                            .order_by()
                            .values("ocid")
                            .distinct()
                        )

                        for item in ocids:
                            # send message to a next phase
                            message = {
                                "ocid": item["ocid"],
                                "collection_id": collection.pk,
                            }
                            self.publish(json.dumps(message))

                    except AlreadyExists:
                        self.warning(
                            """
                            Tried to create already existing compiled collection. This can happen in
                            evironments with multiple compilers running."""
                        )
                else:
                    self.debug("Collection {} is not compilable.".format(collection))
            except CollectionFile.DoesNotExist:
                self.error("Collection file {} not found".format(input_message["collection_file_id"]))

            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            self.exception("Something went wrong when processing {}".format(body))
            sys.exit()
