import json
import traceback

from django.db import transaction

from process.management.commands.base.worker import BaseWorker
from process.models import Collection, CollectionFile, CollectionNote, ProcessingStep, Record, Release
from process.processors.compiler import compilable


class Command(BaseWorker):

    worker_name = "compiler"

    consume_keys = ["file_worker", "collection_closed"]

    def __init__(self):
        super().__init__(self.worker_name)

    def process(self, channel, method, properties, body):
        # parse input message
        input_message = json.loads(body.decode("utf8"))

        try:
            self._debug("Received message {}".format(input_message))

            collection = None
            collection_file = None

            if "collection_id" in input_message:
                # received message form collection closed api endpoint
                collection = Collection.objects.get(pk=input_message["collection_id"])
            else:
                # received message from regular file processing
                collection_file = CollectionFile.objects.prefetch_related("collection").get(
                    pk=input_message["collection_file_id"]
                )

                collection = collection_file.collection

            # next phase can be runnng for hours, ack processing of this message to avoid potential netwrok issues
            channel.basic_ack(delivery_tag=method.delivery_tag)

            if compilable(collection.id):
                if collection.data_type and collection.data_type["format"] == Collection.DataTypes.RELEASE_PACKAGE:
                    real_files_count = CollectionFile.objects.filter(collection=collection).count()
                    if collection.expected_files_count and collection.expected_files_count <= real_files_count:
                        # plans compilation of the whole collection (everything is stored yet)
                        self._publish_releases(collection)
                    else:
                        self._debug("Collection {} is not compilable yet. There are (probably) some"
                                    "unprocessed messages in the queue with the new items"
                                    " - expected files count {} real files count {}".format(
                                        collection,
                                        collection.expected_files_count,
                                        real_files_count))

                if (collection_file and collection.data_type and
                        collection.data_type["format"] == Collection.DataTypes.RECORD_PACKAGE and collection_file):
                    # plans compilation of this file (immedaite compilation - we dont have to wait for all records)
                    self._publish_records(collection_file)
            else:
                self._debug("Collection {} is not compilable.".format(collection))
        except Exception:
            self._exception("Something went wrong when processing {}".format(body))
            try:
                if "collection_id" in input_message:
                    # received message form collection closed api endpoint
                    collection = Collection.objects.get(pk=input_message["collection_id"])
                else:
                    # received message from regular file processing
                    collection_file = CollectionFile.objects.prefetch_related("collection").get(
                        pk=input_message["collection_file_id"]
                    )

                    collection = collection_file.collection

                self._save_note(
                    collection,
                    CollectionNote.Codes.ERROR,
                    "Unable to process message {} \n{}".format(
                        input_message, traceback.format_exc()
                    ),
                )
            except Exception:
                self._exception("Failed saving collection note")

    def _publish_releases(self, collection):
        try:
            with transaction.atomic():
                compiled_collection = (
                    Collection.objects.select_for_update()
                    .filter(transform_type__exact=Collection.Transforms.COMPILE_RELEASES)
                    .filter(compilation_started=False)
                    .get(parent=collection)
                )

                compiled_collection.compilation_started = True
                compiled_collection.save()

            self._info("Planning release compilation for {}".format(compiled_collection))

            # get all ocids for collection
            ocids = (
                Release.objects.filter(collection_file_item__collection_file__collection=collection)
                .order_by()
                .values("ocid")
                .distinct()
            )

            for item in ocids:
                # send message to a next phase
                message = {
                    "ocid": item["ocid"],
                    "collection_id": collection.id,
                }

                self._createStep(ProcessingStep.Types.COMPILE, collection_id=collection.id, ocid=item["ocid"])
                self._publish(json.dumps(message), "compiler_release")
        except Collection.DoesNotExist:
            self._warning(
                """"Tried to plan compilation for already "planned" collection.
                This can rarely happen in multi worker environments."""
            )

    def _publish_records(self, collection_file):
        with transaction.atomic():
            compiled_collection = Collection.objects.select_for_update().filter(
                transform_type__exact=Collection.Transforms.COMPILE_RELEASES
            ).get(parent=collection_file.collection)

            if not compiled_collection.compilation_started:
                compiled_collection.compilation_started = True
                compiled_collection.save()

        self._info("Planning records compilation for {} file {}".format(compiled_collection, collection_file))

        # get all ocids for collection
        ocids = (
            Record.objects.filter(collection_file_item__collection_file=collection_file)
            .order_by()
            .values("ocid")
            .distinct()
        )

        for item in ocids:
            # send message to a next phase
            message = {
                "ocid": item["ocid"],
                "collection_id": collection_file.collection.id,
            }

            self._createStep(ProcessingStep.Types.COMPILE,
                             collection_id=collection_file.collection.id,
                             ocid=item["ocid"])
            self._publish(json.dumps(message), "compiler_record")
