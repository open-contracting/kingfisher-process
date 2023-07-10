import logging
from tempfile import TemporaryDirectory

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from yapw.methods import ack, publish

try:
    from libcoveocds.api import ocds_json_output
    from libcoveocds.config import LibCoveOCDSConfig

    CONFIG = LibCoveOCDSConfig()
    CONFIG.config["standard_zip"] = f"file://{settings.BASE_DIR / '1__1__5.zip'}"
    CONFIG.config["cache_all_requests"] = True
    # Skip empty field checks, covered by Pelican.
    CONFIG.config["additional_checks"] = "none"
    # Skip award reference checks, covered by Pelican, and duplicate release IDs, covered by notebooks.
    CONFIG.config["skip_aggregates"] = True
    CONFIG.config["context"] = "api"

    using_libcoveocds = True
except ImportError:
    using_libcoveocds = False

from process.models import CollectionFile, ProcessingStep, Record, RecordCheck, Release, ReleaseCheck
from process.util import RELEASE_PACKAGE, consume, decorator, delete_step

consume_routing_keys = ["file_worker"]
routing_key = "checker"
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        if not settings.ENABLE_CHECKER:
            raise CommandError("Checker is disabled. Set the ENABLE_CHECKER environment variable to enable.")
        if not using_libcoveocds:
            raise CommandError("Checker is unavailable. Install the libcoveocds Python package.")

        consume(
            on_message_callback=callback, queue=routing_key, routing_keys=consume_routing_keys, decorator=decorator
        )


def callback(client_state, channel, method, properties, input_message):
    collection_id = input_message["collection_id"]
    collection_file_id = input_message["collection_file_id"]

    with delete_step(ProcessingStep.Name.CHECK, collection_file_id=collection_file_id):
        with transaction.atomic():
            collection_file = CollectionFile.objects.select_related("collection").get(pk=collection_file_id)
            if "check" in collection_file.collection.steps:
                check_collection_file(collection_file)

    message = {"collection_id": collection_id, "collection_file_id": collection_file_id}
    publish(client_state, channel, message, routing_key)

    ack(client_state, channel, method.delivery_tag)


def check_collection_file(collection_file):
    """
    Checks releases for a given collection_file.

    :param int collection_file: collection file for which should be releases checked
    """

    logger.info("Checking data for collection file %s", collection_file)

    release_package = (
        collection_file.collection.data_type and collection_file.collection.data_type["format"] == RELEASE_PACKAGE
    )

    if release_package:
        items_key = "releases"
        model = Release
    else:
        items_key = "records"
        model = Record

    items = model.objects.filter(collection_file_item__collection_file=collection_file).select_related(
        "data", "package_data"
    )

    for item in items.iterator():
        logger.debug("Repackaging %s of %s", items_key, item)
        json_data = item.package_data.data
        json_data[items_key] = [item.data.data]

        logger.debug("Checking %s of %s", items_key, item)
        with TemporaryDirectory() as d:
            cove_output = ocds_json_output(
                d,
                "",  # optional if file_type="json", convert=False and json_data is set.
                schema_version="1.1",
                convert=False,
                file_type="json",
                json_data=json_data,
                lib_cove_ocds_config=CONFIG,
                record_pkg=not release_package,
            )

        if release_package:
            check = ReleaseCheck(release=item)
        else:
            check = RecordCheck(record=item)
        check.cove_output = cove_output
        check.save()
