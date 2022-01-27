import logging

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from libcoveocds.api import ocds_json_output
from yapw.methods.blocking import ack, publish

from process.models import Collection, CollectionFile, ProcessingStep, Record, RecordCheck, Release, ReleaseCheck
from process.util import consume, decorator, delete_step

consume_routing_keys = ["file_worker"]
routing_key = "checker"
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        if not settings.ENABLE_CHECKER:
            raise CommandError("Refusing to start as checker is disabled in settings - see ENABLE_CHECKER value.")

        consume(callback, routing_key, consume_routing_keys, decorator=decorator, prefetch_count=20)


@transaction.atomic
def callback(client_state, channel, method, properties, input_message):
    collection_id = input_message["collection_id"]
    collection_file_id = input_message["collection_file_id"]

    collection_file = CollectionFile.objects.select_related("collection").get(pk=collection_file_id)
    if "check" in collection_file.collection.steps:
        check_collection_file(collection_file)

    delete_step(ProcessingStep.Types.CHECK, collection_file_id=collection_file_id)

    message = {
        "collection_id": collection_id,
        "collection_file_id": collection_file_id,
    }
    publish(client_state, channel, message, routing_key)

    ack(client_state, channel, method.delivery_tag)


def check_collection_file(collection_file):
    """
    Checks releases for a given collection_file.

    :param int collection_file: collection file for which should be releases checked
    """

    logger.info("Checking data for collection file %s", collection_file)

    if (
        collection_file.collection.data_type
        and collection_file.collection.data_type["format"] == Collection.DataTypes.RELEASE_PACKAGE
    ):
        items_key = "releases"
        items = Release.objects.filter(collection_file_item__collection_file=collection_file).select_related(
            "data", "package_data"
        )
    else:
        items_key = "records"
        items = Record.objects.filter(collection_file_item__collection_file=collection_file).select_related(
            "data", "package_data"
        )

    for item in items:
        logger.debug("Checking %s form item %s", items_key, item)
        data_to_check = item.data.data
        if item.package_data:
            logger.debug("Repacking for key %s object %s", items_key, item)
            data_to_check = item.package_data.data
            data_to_check[items_key] = item.data.data

        check_result = ocds_json_output(
            "",
            "",
            schema_version="1.0",
            convert=False,
            cache_schema=True,
            file_type="json",
            json_data=data_to_check,
        )

        # eliminate nonrequired check results
        check_result.pop("releases_aggregates", None)
        check_result.pop("records_aggregates", None)

        if items_key == "releases":
            check = ReleaseCheck()
            check.release = item
        else:
            check = RecordCheck()
            check.record = item

        check.cove_output = check_result

        check.save()
