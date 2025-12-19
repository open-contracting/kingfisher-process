import functools
import logging

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils.translation import gettext as t
from libcoveocds.common_checks import common_checks_ocds
from libcoveocds.config import LibCoveOCDSConfig
from libcoveocds.lib.api import context_api_transform
from libcoveocds.schema import SchemaOCDS
from ocdskit.util import Format
from yapw.methods import ack, publish

from process.models import CollectionFile, ProcessingStep, Record, RecordCheck, Release, ReleaseCheck
from process.util import consume, decorator, deleting_step, get_extensions
from process.util import wrap as w

consume_routing_keys = ["file_worker", "addchecks"]
routing_key = "checker"
logger = logging.getLogger(__name__)

CONFIG = LibCoveOCDSConfig()
CONFIG.config["standard_zip"] = f"file://{settings.BASE_DIR / '1__1__5.zip'}"
# No requests are expected to be made by lib-cove, but just in case.
CONFIG.config["cache_all_requests"] = True
# Skip empty field checks, covered by Pelican.
CONFIG.config["additional_checks"] = "none"
CONFIG.config["skip_aggregates"] = True
CONFIG.config["context"] = "api"


class Command(BaseCommand):
    help = w(t("Check collection files"))

    def handle(self, *args, **options):
        if not settings.ENABLE_CHECKER:
            raise CommandError("Checker is disabled. Set the ENABLE_CHECKER environment variable to enable.")

        consume(
            on_message_callback=callback,
            queue=routing_key,
            routing_keys=consume_routing_keys,
            decorator=decorator,
            # 3 hours in milliseconds.
            # https://www.rabbitmq.com/consumers.html
            arguments={"x-consumer-timeout": 3 * 60 * 60 * 1000},
        )


def callback(client_state, channel, method, properties, input_message):
    collection_id = input_message["collection_id"]
    collection_file_id = input_message["collection_file_id"]

    collection_file = CollectionFile.objects.select_related("collection").get(pk=collection_file_id)
    if collection_file.collection.deleted_at:
        ack(client_state, channel, method.delivery_tag)
        return

    with deleting_step(ProcessingStep.Name.CHECK, collection_file_id=collection_file_id), transaction.atomic():
        if "check" in collection_file.collection.steps:
            _check_collection_file(collection_file)

    message = {"collection_id": collection_id, "collection_file_id": collection_file_id}
    publish(client_state, channel, message, routing_key)

    ack(client_state, channel, method.delivery_tag)


# It should be safe to return the instance after instantiation, because all its public methods are also lru_cache'd.
#
# https://github.com/open-contracting/lib-cove-ocds/issues/40#issuecomment-1629456904
@functools.lru_cache
def _get_schema(items_key, extensions):
    # Construct a package to fulfill the initialization logic.
    package_data = {items_key: [], "extensions": list(extensions)}
    return SchemaOCDS("1.1", package_data, lib_cove_ocds_config=CONFIG, record_pkg=items_key == "records")


def _check_collection_file(collection_file):
    logger.info("Collecting data to check for collection file %s", collection_file)

    data_type = collection_file.collection.data_type
    release_package = data_type and data_type["format"] == Format.release_package

    if release_package:
        items_key = "releases"
        model = Release
        related_name = "releasecheck"
    else:
        items_key = "records"
        model = Record
        related_name = "recordcheck"

    items = model.objects.select_related("data", "package_data").filter(
        **{"collection_file": collection_file, f"{related_name}__isnull": True}
    )

    logger.info("Checking %s %s for collection file %s", items.count(), items_key, collection_file)

    for item in items.iterator():
        logger.debug("Repackaging %s of %s", items_key, item)
        package = item.package_data.data
        package[items_key] = [item.data.data]

        # Security: Potential SSRF via extension URLs (within OCDS publication).
        schema = _get_schema(items_key, get_extensions(package))

        logger.debug("Checking %s of %s", items_key, item)
        cove_output = context_api_transform(
            common_checks_ocds(
                # common_checks_context() writes cell_source_map.json and heading_source_map.json if not "json".
                {"file_type": "json"},
                # `upload_dir` is not used.
                "",
                # The data to check.
                package,
                # Cache SchemaOCDS instances by package type and extensions list.
                schema,
                # common_checks_context(cache=True) caches the results to a file, which is not needed in API context.
                cache=False,
            )
        )
        if schema.json_deref_error:
            cove_output["json_deref_error"] = schema.json_deref_error

        check = ReleaseCheck(release=item) if release_package else RecordCheck(record=item)
        check.cove_output = cove_output
        check.save()

    logger.info("Checked data for collection file %s", collection_file)
