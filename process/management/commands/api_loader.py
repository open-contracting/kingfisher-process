import json
import os.path

from django.conf import settings
from django.db import transaction
from django.db.models.functions import Now

from process.management.commands.base.worker import BaseWorker
from process.models import Collection
from process.processors.loader import create_collection_file
from process.util import json_dumps


class Command(BaseWorker):
    worker_name = "api_loader"
    consume_keys = ["api"]
    prefetch_count = 20

    def __init__(self):
        super().__init__(self.worker_name)

    def process(self, connection, channel, delivery_tag, body):
        input_message = json.loads(body.decode("utf8"))
        if input_message.get("errors", None) and not input_message.get("path", None):
            input_message["path"] = input_message.get("url", None)
        else:
            input_message["path"] = os.path.join(settings.KINGFISHER_COLLECT_FILES_STORE, input_message["path"])

        collection_id = input_message["collection_id"]

        try:
            collection = Collection.objects.get(pk=collection_id)
            with transaction.atomic():
                collection_file = create_collection_file(
                    collection,
                    file_path=input_message.get("path", None),
                    url=input_message.get("url", None),
                    errors=input_message.get("errors", None),
                )

                message = {"collection_id": collection_id, "collection_file_id": collection_file.pk}

                if input_message.get("close", False):
                    # close collections as well
                    collection = Collection.objects.select_for_update().get(pk=collection_id)
                    collection.store_end_at = Now()
                    collection.save()

                    upgraded_collection = collection.get_upgraded_collection()
                    if upgraded_collection:
                        upgraded_collection.store_end_at = Now()
                        upgraded_collection.save()

            if "errors" not in input_message:
                # only files without errors will be further processed
                self._publish_async(connection, channel, json_dumps(message))
            else:
                self.logger.info(
                    "Collection file %s contains errors %s, not sending to further processing.",
                    collection_file,
                    input_message.get("errors", None),
                )

        except Collection.DoesNotExist:
            self.logger.exception("Collection with id %s not found", collection_id)
        except Exception:
            self.logger.exception("Unable to create collection_file")

        # confirm message processing
        self._ack(connection, channel, delivery_tag)
        self._clean_thread_resources()
