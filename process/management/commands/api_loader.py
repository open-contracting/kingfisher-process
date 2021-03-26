import json

from django.db import transaction
from django.db.models.functions import Now

from process.management.commands.base.worker import BaseWorker
from process.models import Collection
from process.processors.loader import create_collection_file
from process.util import json_dumps


class Command(BaseWorker):

    worker_name = "api_loader"

    consume_keys = ["api"]

    def __init__(self):
        super().__init__(self.worker_name)

    def process(self, channel, method, properties, body):
        # parse input message
        input_message = json.loads(body.decode("utf8"))

        try:
            collection = Collection.objects.get(id=input_message["collection_id"])
            with transaction.atomic():
                collection_file = create_collection_file(collection,
                                                         file_path=input_message.get("path", None),
                                                         url=input_message.get("url", None),
                                                         errors=input_message.get("errors", None))

                message = {"collection_file_id": collection_file.id}

                if input_message.get("close", False):
                    # close collections as well
                    collection = Collection.objects.get(id=input["collection_id"])
                    collection.store_end_at = Now()
                    collection.save()

                    upgraded_collection = collection.get_upgraded_collection()
                    if upgraded_collection:
                        upgraded_collection.store_end_at = Now()
                        upgraded_collection.save()

            if "errors" not in input_message:
                # only files without errors will be further processed
                self._publish(json_dumps(message))

        except Collection.DoesNotExist:
            self._exception("Collection with id {} not found".format(input["collection_id"]))
        except Exception as e:
            self._exception("Unable to create collection_file", e)

        # confirm message processing
        channel.basic_ack(delivery_tag=method.delivery_tag)
