import json
import sys

from process.management.commands.base.worker import BaseWorker
from process.models import Collection, CollectionFile, CollectionFileStep, Release


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

                if self.proceed(collection):
                    try:
                        compile_collection = Collection.objects.filter(parent=collection).get(
                            parent__steps__contains="compile"
                        )
                    except Collection.DoesNotExist:
                        # create collection
                        compile_collection = Collection()
                        compile_collection.parent = collection
                        compile_collection.steps = []
                        compile_collection.source_id = collection.source_id
                        compile_collection.data_version = collection.data_version
                        compile_collection.sample = collection.sample
                        compile_collection.expected_files_count = collection.expected_files_count
                        compile_collection.transform_type = Collection.Transforms.COMPILE_RELEASES
                        compile_collection.cached_releases_count = collection.cached_releases_count
                        compile_collection.cached_records_count = collection.cached_records_count
                        compile_collection.cached_compiled_releases_count = collection.cached_compiled_releases_count
                        compile_collection.store_start_at = collection.store_start_at
                        compile_collection.store_end_at = collection.store_end_at
                        compile_collection.deleted_at = collection.deleted_at
                        compile_collection.save()

                        self.info("Compiling releases")

                        ocids = (
                            Release.objects.filter(collection_file_item__collection_file__collection=collection)
                            .order_by()
                            .values("ocid")
                            .distinct()
                        )

                        for item in ocids:
                            # send message for a next phase
                            message = {
                                "ocid": item["ocid"],
                                "collection_id": collection.pk,
                            }
                            self.publish(json.dumps(message))

                # confirm message processing
            except CollectionFile.DoesNotExist:
                self.warning("Collection file {} not found".format(input_message["collection_file_id"]))

            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            self.exception("Something went wrong when processing {}".format(body))
            sys.exit()
