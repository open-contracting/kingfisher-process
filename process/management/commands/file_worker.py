import json
import traceback

from django.db import IntegrityError, transaction

from process.management.commands.base.worker import BaseWorker
from process.models import Collection, CollectionNote, ProcessingStep
from process.processors.file_loader import process_file
from process.util import json_dumps


class Command(BaseWorker):

    worker_name = "file_worker"

    consume_keys = ["loader", "api"]

    def __init__(self):
        super().__init__(self.worker_name)

    def process(self, channel, method, properties, body):
        # parse input message
        input_message = json.loads(body.decode("utf8"))

        try:
            self._debug("Received message {}".format(input_message))

            collection_file_id = input_message["collection_file_id"]
            upgraded_collection_file_id = None
            with transaction.atomic():
                upgraded_collection_file_id = process_file(collection_file_id)

                self._deleteStep(ProcessingStep.Types.LOAD, collection_file_id=collection_file_id)

            self._createStep(ProcessingStep.Types.CHECK, collection_file_id=collection_file_id)
            self._publish(json.dumps(input_message))

            # send upgraded collection file to further processing
            if upgraded_collection_file_id:
                message = {"collection_file_id": upgraded_collection_file_id}
                self._createStep(ProcessingStep.Types.CHECK, collection_file_id=upgraded_collection_file_id)
                self._publish(json_dumps(message))

            # confirm message processing
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except IntegrityError:
            self._exception(
                """ This should be a very rare exception, most probably one worker stored
                    data item during processing the very same data in current worker.
                    Message body {}""".format(
                    body
                )
            )

            # return message to queue
            channel.basic_nack(delivery_tag=method.delivery_tag)
        except Exception:
            collection_file_id = input_message["collection_file_id"]

            self._exception("Something went wrong when processing {}".format(body))
            try:
                collection = Collection.objects.get(collectionfile__id=collection_file_id)
                self._save_note(
                    collection,
                    CollectionNote.Codes.ERROR,
                    "Unable to process collection_file_id {} \n{}".format(
                        input_message["collection_file_id"], traceback.format_exc()
                    ),
                )
            except Exception:
                self._exception("Failed saving collection note")

            self._deleteStep(ProcessingStep.Types.LOAD, collection_file_id=collection_file_id)

            # confirm message processing
            channel.basic_ack(delivery_tag=method.delivery_tag)
