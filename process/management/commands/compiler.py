import json
import sys

from ocdsmerge import Merger

from process.management.commands.base.worker import BaseWorker
from process.models import (Collection, CollectionFile, CollectionFileItem, CollectionFileStep, CompiledRelease, Data,
                            Release)
from process.util import get_hash


class Command(BaseWorker):

    worker_name = "compiler"

    consume_keys = ["file_worker"]

    def __init__(self):
        super().__init__(self.worker_name)

    def process(self, channel, method, properties, body):
        try:
            # parse input message
            input_message = json.loads(body.decode('utf8'))

            self.debug("Received message {}".format(input_message))

            try:
                collection_file = CollectionFile.objects.prefetch_related('collection').get(
                    pk=input_message["collection_file_id"])

                collection = collection_file.collection

                if self.proceed(collection):
                    try:
                        compile_collection = Collection.objects.filter(
                                                parent=collection).get(steps__contains="compile")
                    except Collection.DoesNotExist:
                        # create collection
                        compile_collection = Collection()
                        compile_collection.parent = collection
                        compile_collection.steps = []
                        compile_collection.source_id = collection.source_id
                        compile_collection.data_version = collection.data_version
                        compile_collection.sample = collection.sample
                        compile_collection.expected_files_count = collection.expected_files_count
                        compile_collection.parent = collection.parent
                        compile_collection.transform_type = Collection.Transforms.COMPILE_RELEASES
                        compile_collection.cached_releases_count = collection.cached_releases_count
                        compile_collection.cached_records_count = collection.cached_records_count
                        compile_collection.cached_compiled_releases_count = collection.cached_compiled_releases_count
                        compile_collection.store_start_at = collection.store_start_at
                        compile_collection.store_end_at = collection.store_end_at
                        compile_collection.deleted_at = collection.deleted_at
                        compile_collection.save()

                        self.info("Compiling releases")

                        ocids = Release.objects.filter(
                                    collection_file_item__collection_file__collection=collection).order_by(
                                    ).values('ocid').distinct()

                        for ocid in ocids:
                            releases = Release.objects.filter(
                                            collection_file_item__collection_file__collection=collection).filter(
                                            ocid=ocid['ocid']).order_by().prefetch_related('data')
                            releases_data = []
                            for release in releases:
                                releases_data.append(release.data.data)

                            merger = Merger()
                            compiled_release = merger.create_compiled_release(releases_data)
                            compiled_collection_file = CollectionFile()
                            compiled_collection_file.collection = compile_collection
                            compiled_collection_file.filename = collection_file.filename + ocid['ocid']
                            compiled_collection_file.url = collection_file.url
                            compiled_collection_file.save()

                            collection_file_item = CollectionFileItem()
                            collection_file_item.collection_file = compiled_collection_file
                            collection_file_item.number = 0
                            collection_file_item.save()

                            compiled_release_hash = get_hash(str(compiled_release))

                            try:
                                data = Data.objects.get(hash_md5=compiled_release_hash)
                            except (Data.DoesNotExist, Data.MultipleObjectsReturned):
                                data = Data()
                                data.data = compiled_release
                                data.hash_md5 = compiled_release_hash
                                data.save()

                            release = CompiledRelease()
                            release.collection = compile_collection
                            release.collection_file_item = collection_file_item
                            release.data = data
                            release.ocid = compiled_release["ocid"]
                            release.save()

                        # send message for a next phase
                        message = {"dataset_id": 1}
                        self.publish(json.dumps(message))

                # confirm message processing
            except CollectionFile.DoesNotExist:
                self.warning("Collection file {} not found".format(input_message["collection_file_id"]))

            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            self.exception(
                "Something went wrong when processing {}".format(body))
            sys.exit()

    def proceed(self, collection):
        if "compile" in collection.steps and collection.store_end_at is not None:
            collection_file_step_count = CollectionFileStep.objects.filter(
                collection_file__collection=collection).filter(name="file_checker").count()
            if collection_file_step_count == 0:
                return True

        return False
