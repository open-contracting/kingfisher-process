import json
import traceback

from django.conf import settings
from django.db import IntegrityError, transaction

from process.management.commands.base.worker import BaseWorker
from process.models import Collection, CollectionNote, ProcessingStep
from process.processors.file_loader import process_file
from process.util import json_dumps


class Command(BaseWorker):
    worker_name = "file_worker"
    consume_keys = ["loader", "api_loader"]
    prefetch_count = 20

    def __init__(self):
        super().__init__(self.worker_name)

    def process(self, connection, channel, delivery_tag, body):
        input_message = json.loads(body.decode("utf8"))

        collection_id = input_message["collection_id"]
        collection_file_id = input_message["collection_file_id"]

        try:
            self.logger.debug("Received message %s", input_message)

            upgraded_collection_file_id = None

            message = {
                "collection_id": input_message["collection_id"],
                "collection_file_id": input_message["collection_file_id"]
            }

            with transaction.atomic():
                upgraded_collection_file_id = process_file(collection_file_id)

                self._delete_step(ProcessingStep.Types.LOAD, collection_file_id=collection_file_id)

            if settings.ENABLE_CHECKER:
                self._create_step(ProcessingStep.Types.CHECK, collection_id, collection_file_id=collection_file_id)

            self._publish_async(connection, channel, json.dumps(message))

            # send upgraded collection file to further processing
            if upgraded_collection_file_id:
                message["collection_file_id"] = upgraded_collection_file_id

                if settings.ENABLE_CHECKER:
                    self._create_step(
                        ProcessingStep.Types.CHECK, collection_id, collection_file_id=upgraded_collection_file_id
                    )

                self._publish_async(connection, channel, json_dumps(message))

            # confirm message processing
            self._ack(connection, channel, delivery_tag)
        except IntegrityError:
            self.logger.exception(
                "This should be a very rare exception, most probably one worker stored data item during processing "
                "the very same data in current worker. Message body %s",
                body,
            )

            # return message to queue
            self._nack(connection, channel, delivery_tag)
        except Exception:
            self.logger.exception("Something went wrong when processing %s", body)
            try:
                collection = Collection.objects.get(collectionfile__pk=collection_file_id)
                self._save_note(
                    collection,
                    CollectionNote.Codes.ERROR,
                    f"Unable to process collection_file_id {collection_file_id}\n{traceback.format_exc()}",
                )
            except Exception:
                self.logger.exception("Failed saving collection note")

            self._delete_step(ProcessingStep.Types.LOAD, collection_file_id=collection_file_id)

            # confirm message processing
            self._ack(connection, channel, delivery_tag)

        self._clean_thread_resources()
