import logging
from collections import OrderedDict

import ijson
import simplejson as json
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.utils import IntegrityError
from ijson.common import ObjectBuilder
from ocdskit.exceptions import UnknownFormatError
from ocdskit.upgrade import upgrade_10_11
from ocdskit.util import detect_format
from yapw.methods import ack, nack, publish

from process.exceptions import UnsupportedFormatError
from process.models import (
    CollectionFile,
    CollectionFileItem,
    CollectionNote,
    Data,
    PackageData,
    ProcessingStep,
    Record,
    Release,
)
from process.util import (
    RECORD_PACKAGE,
    RELEASE_PACKAGE,
    consume,
    create_note,
    create_step,
    decorator,
    delete_step,
    get_hash,
)

consume_routing_keys = ["loader", "api_loader"]
routing_key = "file_worker"
logger = logging.getLogger(__name__)

SUPPORTED_FORMATS = [RELEASE_PACKAGE, RECORD_PACKAGE]


class Command(BaseCommand):
    def handle(self, *args, **options):
        consume(
            on_message_callback=callback, queue=routing_key, routing_keys=consume_routing_keys, decorator=decorator
        )


def callback(client_state, channel, method, properties, input_message):
    collection_id = input_message["collection_id"]
    collection_file_id = input_message["collection_file_id"]

    try:
        with delete_step(ProcessingStep.Name.LOAD, collection_file_id=collection_file_id):
            with transaction.atomic():
                collection_file = CollectionFile.objects.select_related("collection").get(pk=collection_file_id)
                collection = collection_file.collection
                upgraded_collection_file_id = process_file(collection_file)

        # If a duplicate message is received causing an IntegrityError above, we still want to create the next step, in
        # case it was not created the first time. delete_step() will delete any duplicate steps.
        if settings.ENABLE_CHECKER:
            create_step(ProcessingStep.Name.CHECK, collection_id, collection_file_id=collection_file_id)

        message = {"collection_id": collection_id, "collection_file_id": collection_file_id}
        publish(client_state, channel, message, routing_key)

        if upgraded_collection_file_id:
            if settings.ENABLE_CHECKER:
                create_step(ProcessingStep.Name.CHECK, collection_id, collection_file_id=upgraded_collection_file_id)

            message = {"collection_id": collection_id, "collection_file_id": upgraded_collection_file_id}
            publish(client_state, channel, message, routing_key)
    # Irrecoverable errors. Discard the message to allow other messages to be processed.
    except (UnknownFormatError, UnsupportedFormatError):  # raised by detect_format() or process_file()
        logger.exception("Source %s yields an unknown or unsupported format, skipping", collection.source_id)
        create_note(collection, CollectionNote.Level.ERROR, f"Source {collection.source_id} yields unknown format")
        nack(client_state, channel, method.delivery_tag, requeue=False)
    except ijson.common.IncompleteJSONError:  # raised by ijson.parse()
        logger.exception("Source %s yields invalid JSON, skipping", collection.source_id)
        create_note(collection, CollectionNote.Level.ERROR, f"Source {collection.source_id} yields invalid JSON")
        nack(client_state, channel, method.delivery_tag, requeue=False)
    else:
        ack(client_state, channel, method.delivery_tag)


def process_file(collection_file):
    """
    Loads file for a given collection - created the whole collection_file, collection_file_item etc. structure.
    If the collection should be upgraded, creates the same structure for upgraded collection as well.

    :param collection_file: collection file for which should be releases checked

    :returns: upgraded collection file id or None (if there is no upgrade planned)
    :rtype: int
    """

    data_type = _get_data_type(collection_file)

    if data_type["format"] not in SUPPORTED_FORMATS:
        raise UnsupportedFormatError(
            f"Unsupported data type '{data_type}' for file {collection_file}. Must be one of {SUPPORTED_FORMATS}."
        )

    package, releases_or_records = _read_data_from_file(collection_file.filename, data_type)

    logger.debug("Writing data for collection_file %s", collection_file.pk)
    _store_data(collection_file, package, releases_or_records, data_type, upgrade=False)

    upgraded_collection = collection_file.collection.get_upgraded_collection()
    if upgraded_collection:
        upgraded_collection_file = CollectionFile(
            collection=upgraded_collection, filename=collection_file.filename, url=collection_file.url
        )
        upgraded_collection_file.save()

        logger.debug("Writing data for upgraded collection_file %s", upgraded_collection_file.pk)
        _store_data(upgraded_collection_file, package, releases_or_records, data_type, upgrade=True)

        return upgraded_collection_file.pk


def _get_data_type(collection_file):
    """
    Returns the expected data type of the collection_file.
    """
    collection = collection_file.collection
    if not collection.data_type:
        detected_format = detect_format(collection_file.filename)
        data_type = {
            "format": detected_format[0],
            "concatenated": detected_format[1],
            "array": detected_format[2],
        }

        collection.data_type = data_type
        collection.save(update_fields=["data_type"])

        upgraded_collection = collection.get_upgraded_collection()
        if upgraded_collection:
            upgraded_collection.data_type = data_type
            upgraded_collection.save(update_fields=["data_type"])

    return collection.data_type


class ControlCodesFilter:
    def __init__(self, file):
        self.file = file

    def read(self, buf_size):
        # Replace the "\u0000" escape sequence in the JSON string, which is rejected by PostgreSQL.
        # https://www.postgresql.org/docs/current/datatype-json.html
        return self.file.read(buf_size).replace(b"\\u0000", b"")


def _read_data_from_file(filename, data_type):
    package_key = ""
    data_key = ""

    if data_type["array"]:
        package_key = "item"
        data_key = "item."

    if data_type["format"] == RECORD_PACKAGE:
        data_key += "records"
    elif data_type["format"] == RELEASE_PACKAGE:
        data_key += "releases"

    with open(filename, "rb") as f:
        build_package = False
        build_data = False

        package_builder = ObjectBuilder()
        data_builder = ObjectBuilder()

        package = None
        releases_or_records = []

        # Constructs Decimal values. https://github.com/ICRAR/ijson#options
        for prefix, event, value in ijson.parse(ControlCodesFilter(f)):
            if prefix == package_key:
                # Start of package.
                if event == "start_map":
                    build_package = True
                    package_builder = ObjectBuilder()
                # End of package.
                elif event == "end_map":
                    build_package = False
                    package = package_builder.value
            elif prefix == f"{data_key}.item":
                # Start of an item of data.
                if event == "start_map":
                    build_data = True
                    data_builder = ObjectBuilder()
                # End of an item of data.
                elif event == "end_map":
                    build_data = False
                    releases_or_records.append(OrderedDict(data_builder.value))

            if build_package:
                if not prefix.startswith(data_key):
                    package_builder.event(event, value)
            if build_data:
                data_builder.event(event, value)

    return package, releases_or_records


def _store_data(collection_file, package, releases_or_records, data_type, upgrade=False):
    collection_file_item = CollectionFileItem(collection_file=collection_file, number=0)
    collection_file_item.save()

    package_data = _store_deduplicated_data(PackageData, package)

    for release_or_record in releases_or_records:
        if upgrade:
            # upgrade_10_11() requires an OrderedDict. simplejson is used for native decimal support.
            release_or_record = upgrade_10_11(
                json.loads(json.dumps(release_or_record, use_decimal=True), object_pairs_hook=OrderedDict)
            )

        data = _store_deduplicated_data(Data, release_or_record)

        # The ocid is required to find all the releases relating to the same record, during compilation.
        if "ocid" not in release_or_record:
            logger.error("Skipped release or record without ocid: %s", release_or_record)
            continue

        if data_type["format"] == RECORD_PACKAGE:
            Record(
                collection=collection_file.collection,
                collection_file_item=collection_file_item,
                package_data=package_data,
                data=data,
                ocid=release_or_record["ocid"],
            ).save()
        elif data_type["format"] == RELEASE_PACKAGE:
            Release(
                collection=collection_file.collection,
                collection_file_item=collection_file_item,
                package_data=package_data,
                data=data,
                ocid=release_or_record["ocid"],
                release_id=release_or_record["id"],
            ).save()


def _store_deduplicated_data(model, data):
    hash_md5 = get_hash(data)

    try:
        obj = model.objects.get(hash_md5=hash_md5)
    except (model.DoesNotExist, model.MultipleObjectsReturned):
        obj = model(data=data, hash_md5=hash_md5)
        try:
            with transaction.atomic():
                obj.save()
        except IntegrityError:
            obj = model.objects.get(hash_md5=hash_md5)

    return obj
