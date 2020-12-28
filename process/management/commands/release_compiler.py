import json
import sys

from django.db import transaction
from ocdsmerge import Merger

from process.management.commands.base.worker import BaseWorker
from process.models import (
    Collection,
    CollectionFile,
    CollectionFileItem,
    CollectionFileStep,
    CompiledRelease,
    Data,
    Release,
)
from process.util import get_hash


class Command(BaseWorker):
    """
    The worker is responsible for the compilation of particular releases.
    Consumes messages with an ocid and collection_id which should be compiled.
    The whole structure of CollectionFile, CollectionFileItem, and CompiledRelease
    is created and saved.
    It's safe to run multiple workers of this type at the same type.
    """

    worker_name = "release_compiler"

    consume_keys = ["compiler"]

    def __init__(self):
        super().__init__(self.worker_name)

    def process(self, channel, method, properties, body):
        try:
            # parse input message
            input_message = json.loads(body.decode("utf8"))

            self.debug("Received message {}".format(input_message))

            ocid = input_message["ocid"]
            collection_id = input_message["collection_id"]

            self.info(
                "Compiling release collection_id: {} ocid: {}".format(
                    collection_id, ocid
                )
            )

            # retrieve collection including its parent from db
            collection = Collection.objects.prefetch_related("parent").get(
                pk=collection_id
            )

            # get all releases for given ocid
            releases = (
                Release.objects.filter(
                    collection_file_item__collection_file__collection=collection
                )
                .filter(ocid=ocid)
                .order_by()
                .prefetch_related("data")
            )

            # create array with all the data for releases
            releases_data = []
            for release in releases:
                releases_data.append(release.data.data)

            # merge data into into single compiled release
            merger = Merger()
            compiled_release = merger.create_compiled_release(releases_data)

            with transaction.atomic():
                compiled_collection_file = CollectionFile()
                compiled_collection_file.collection = collection
                compiled_collection_file.filename = ocid
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
                release.collection = collection
                release.collection_file_item = collection_file_item
                release.data = data
                release.ocid = ocid
                release.save()

            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            self.exception("Something went wrong when processing {}".format(body))
            sys.exit()

    def proceed(self, collection):
        if "compile" in collection.steps and collection.store_end_at is not None:
            collection_file_step_count = (
                CollectionFileStep.objects.filter(
                    collection_file__collection=collection
                )
                .filter(name="file_checker")
                .count()
            )
            if collection_file_step_count == 0:
                return True

        return False
