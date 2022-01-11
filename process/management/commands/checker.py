import json
import traceback

from django.conf import settings
from django.core.management.base import CommandError
from django.db import transaction

from process.exceptions import AlreadyExists
from process.management.commands.base.worker import BaseWorker
from process.models import Collection, CollectionFile, CollectionNote, ProcessingStep
from process.processors.checker import check_collection_file


class Command(BaseWorker):
    worker_name = "checker"
    consume_keys = ["file_worker"]
    prefetch_count = 20

    def __init__(self):
        super().__init__(self.worker_name)

        if not settings.ENABLE_CHECKER:
            raise CommandError("Refusing to start as checker is disabled in settings - see ENABLE_CHECKER value.")

    def process(self, connection, channel, delivery_tag, body):
        input_message = json.loads(body.decode("utf8"))
        try:

            self.logger.debug("Received message %s", input_message)

            collection_file = CollectionFile.objects.select_related("collection").get(
                pk=input_message["collection_file_id"]
            )

            if "check" in collection_file.collection.steps:
                try:
                    with transaction.atomic():
                        check_collection_file(collection_file)
                except AlreadyExists:
                    self.logger.exception("Checks already calculated for collection file %s", collection_file)
                    self._save_note(
                        collection_file.collection,
                        CollectionNote.Codes.WARNING,
                        "Checks already calculated for collection file {}".format(collection_file),
                    )

                self.logger.info("Checks calculated for collection file %s", collection_file)
            else:
                self.logger.info("Collection file %s is not checkable. Skip.", collection_file)

            self._delete_step(ProcessingStep.Types.CHECK, collection_file_id=collection_file.pk)

            message = {
                "collection_file": collection_file.pk,
                "collection_id": collection_file.collection.pk,
            }

            self._publish_async(connection, channel, json.dumps(message))
        except Exception:
            self.logger.exception("Something went wrong when processing %s", body)
            try:
                collection = Collection.objects.get(collectionfile__pk=input_message["collection_file_id"])
                self._save_note(
                    collection,
                    CollectionNote.Codes.ERROR,
                    "Unable to process collection file id {} \n{}".format(
                        input_message["collection_file_id"], traceback.format_exc()
                    ),
                )
            except Exception:
                self.logger.exception("Failed saving collection note")

        self._ack(connection, channel, delivery_tag)
        self._clean_thread_resources()
