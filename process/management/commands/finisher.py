import json
import traceback

from django.db import transaction
from django.db.models.functions import Now

from process.management.commands.base.worker import BaseWorker
from process.models import Collection, CollectionNote
from process.processors.finisher import completable


class Command(BaseWorker):
    """
    The worker is responsible for the final steps in collection processing.
    All the checks and calculations are done at this moment, collection is fully procesed.
    Practically, only the completed status is set on collection.
    """

    worker_name = "finisher"

    # Read all messages that might be the final message. "file_worker" can be the final message if neither checking nor
    # compiling are performed, and if the "collection_closed" message is processed before the "file_worker" message.
    consume_keys = ["file_worker", "checker", "release_compiler", "record_compiler", "collection_closed"]

    def __init__(self):
        super().__init__(self.worker_name)

    def process(self, connection, channel, delivery_tag, body):
        input_message = json.loads(body.decode("utf8"))

        try:
            self.logger.debug("Received message %s", input_message)

            collection_id = input_message["collection_id"]

            with transaction.atomic():
                if completable(collection_id):
                    collection = Collection.objects.select_for_update().get(pk=input_message["collection_id"])
                    if collection.transform_type == Collection.Transforms.COMPILE_RELEASES:
                        collection.store_end_at = Now()

                    collection.completed_at = Now()
                    collection.save()

                    # complete upgraded collection as well
                    upgraded_collection = collection.get_upgraded_collection()
                    if upgraded_collection:
                        upgraded_collection.completed_at = Now()
                        upgraded_collection.save()

                    self.logger.debug("Processing of collection_id: %s finished. Set as completed.", collection_id)
                else:
                    self.logger.debug("Processing of collection_id: %s not completable", collection_id)

        except Exception:
            self.logger.exception("Something went wrong when processing %s", body)
            try:
                collection = Collection.objects.get(pk=input_message["collection_id"])
                self._save_note(
                    collection,
                    CollectionNote.Codes.ERROR,
                    "Unable to process message for collection id : {} \n{}".format(
                        input_message["collection_id"], traceback.format_exc()
                    ),
                )
            except Exception:
                self.logger.exception("Failed saving collection note")

        self._ack(connection, channel, delivery_tag)

        self._clean_thread_resources()
