import json

from process.management.commands.base.worker import BaseWorker
from process.models import Collection


class Command(BaseWorker):

    worker_name = "wiper"

    consume_keys = ["wiper"]

    def __init__(self):
        super().__init__(self.worker_name)

    def process(self, connection, channel, delivery_tag, body):
        # parse input message
        input_message = json.loads(body.decode("utf8"))
        try:
            self._debug("Received message %s", input_message)

            collection_id = input_message["collection_id"]

            collection = Collection.objects.get(pk=collection_id)
            self._debug("Deleting collection %s", collection)

            collection.delete()

            self._info("Collection %s successfully wiped.", collection)
        except Collection.DoesNotExist:
            error = "Collection with id {} not found".format(input_message["collection_id"])
            self._error(error)

        self._ack(connection, channel, delivery_tag)
