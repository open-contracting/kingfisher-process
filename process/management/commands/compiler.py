import json
import traceback

from django.db import transaction

from process.management.commands.base.worker import BaseWorker
from process.models import Collection, CollectionFile, CollectionNote, ProcessingStep, Record, Release
from process.processors.compiler import compilable


class Command(BaseWorker):

    worker_name = "compiler"

    consume_keys = ["file_worker"]

    def __init__(self):
        super().__init__(self.worker_name)

    def process(self, channel, method, properties, body):
        # parse input message
        input_message = json.loads(body.decode("utf8"))

        try:
            self._debug("Received message {}".format(input_message))

            collection_file = CollectionFile.objects.prefetch_related("collection").get(
                pk=input_message["collection_file_id"]
            )

            collection = collection_file.collection

            if compilable(collection.id):
                if collection.data_type and collection.data_type["format"] == Collection.DataTypes.RELEASE_PACKAGE:
                    self._publish_releases(collection)
                else:
                    self._publish_records(collection_file)
            else:
                self._debug("Collection {} is not compilable.".format(collection))
        except Exception:
            self._exception("Something went wrong when processing {}".format(body))
            try:
                collection = Collection.objects.get(collectionfile__id=input_message["collection_file_id"])
                self._save_note(
                    collection,
                    CollectionNote.Codes.ERROR,
                    "Unable to process collection_file_id {} \n{}".format(
                        input_message["collection_file_id"], traceback.format_exc()
                    ),
                )
            except Exception:
                self._exception("Failed saving collection note")

        channel.basic_ack(delivery_tag=method.delivery_tag)

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
        compiled_collection = Collection.objects.filter(
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