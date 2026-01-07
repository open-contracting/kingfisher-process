import logging
import random
import time
from collections import OrderedDict

import ijson
import simplejson as json
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import OperationalError, transaction
from django.utils.translation import gettext as t
from ijson.common import ObjectBuilder
from ocdskit.exceptions import UnknownFormatError
from ocdskit.upgrade import upgrade_10_11
from ocdskit.util import Format, detect_format
from yapw.methods import ack, nack, publish

from process.exceptions import EmptyFormatError, UnsupportedFormatError
from process.models import (
    CollectionFile,
    CollectionNote,
    CompiledRelease,
    Data,
    PackageData,
    ProcessingStep,
    Record,
    Release,
)
from process.util import (
    consume,
    create_logger_note,
    create_note,
    create_step,
    decorator,
    delete_step,
    deleting_step,
    get_or_create,
)
from process.util import wrap as w

consume_routing_keys = ["loader", "api_loader"]
routing_key = "file_worker"
logger = logging.getLogger(__name__)

SUPPORTED_FORMATS = {Format.release_package, Format.record_package, Format.compiled_release}
ERROR = CollectionNote.Level.ERROR
MAX_ATTEMPTS = 5


class Command(BaseCommand):
    help = w(t("Create releases, records and compiled releases"))

    def handle(self, *args, **options):
        consume(
            on_message_callback=callback,
            queue=routing_key,
            routing_keys=consume_routing_keys,
            decorator=decorator,
            # 3 hours in milliseconds.
            # https://www.rabbitmq.com/consumers.html
            arguments={"x-consumer-timeout": 3 * 60 * 60 * 1000},
        )


def finish(collection_id, collection_file_id, exception):
    # If a duplicate message is received causing an IntegrityError or similar, we still want to create the next step,
    # in case it was not created the first time. deleting_step() will delete any duplicate steps.
    #
    # See the try/except block in the callback() function of the file_worker worker.
    if settings.ENABLE_CHECKER and not isinstance(exception, FileNotFoundError | ijson.common.IncompleteJSONError):
        create_step(ProcessingStep.Name.CHECK, collection_id, collection_file_id=collection_file_id)


def callback(client_state, channel, method, properties, input_message):
    collection_id = input_message["collection_id"]
    collection_file_id = input_message["collection_file_id"]

    try:
        collection_file = CollectionFile.objects.select_related("collection").get(pk=collection_file_id)
    except CollectionFile.DoesNotExist:
        ack(client_state, channel, method.delivery_tag)
        return

    collection = collection_file.collection
    if collection.deleted_at:
        ack(client_state, channel, method.delivery_tag)
        return

    try:
        # Detect and save the data_type before the transaction, to avoid locking collection rows during process_file().
        #
        # NOTE: Kingfisher Process assumes a single format for all collection files. Unknown, unsupported, or empty
        # formats are detected only for the first collection files processed.
        try:
            set_data_type(collection, collection_file)
        except (UnknownFormatError, UnsupportedFormatError) as e:  # UnknownFormatError is raised by detect_format()
            logger.exception("Source %s yields an unknown or unsupported format, skipping", collection.source_id)
            delete_step(ProcessingStep.Name.LOAD, collection_file_id=collection_file_id)
            create_note(
                collection,
                CollectionNote.Level.ERROR,
                f"Source {collection.source_id} yields an unknown or unsupported format",
                data={"type": type(e).__name__, **input_message},
            )
            nack(client_state, channel, method.delivery_tag, requeue=False)
            return
        except EmptyFormatError as e:
            # Don't log a message, since sources with empty packages also have non-empty packages.
            delete_step(ProcessingStep.Name.LOAD, collection_file_id=collection_file_id)
            create_note(
                collection,
                CollectionNote.Level.WARNING,
                str(e),
                data={"type": type(e).__name__, **input_message},
            )
            nack(client_state, channel, method.delivery_tag, requeue=False)
            return

        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                with (
                    deleting_step(
                        ProcessingStep.Name.LOAD,
                        collection_file_id=collection_file_id,
                        finish=finish,
                        finish_args=(collection_id, collection_file_id),
                    ),
                    transaction.atomic(),
                ):
                    upgraded_collection_file_id = process_file(collection_file)
            # If another transaction in another thread INSERTs the same data, concurrently.
            except OperationalError as e:
                logger.warning("Deadlock on %s %s (%d/%d)\n%s", collection, collection_file, attempt, MAX_ATTEMPTS, e)
                if attempt == MAX_ATTEMPTS:
                    raise
                # Make the threads retry at different times, to avoid repeating the deadlock.
                time.sleep(random.randint(1, 5))  # noqa: S311 # non-cryptographic
            else:
                break

        message = {"collection_id": collection_id, "collection_file_id": collection_file_id}
        publish(client_state, channel, message, routing_key)

        if upgraded_collection_file_id:
            # The deleting_step() context manager sets upgraded_collection_file_id only if successful, so we can create
            # this step here instead of in the finish() function.
            if settings.ENABLE_CHECKER:
                create_step(ProcessingStep.Name.CHECK, collection_id, collection_file_id=upgraded_collection_file_id)

            message = {"collection_id": collection_id, "collection_file_id": upgraded_collection_file_id}
            publish(client_state, channel, message, routing_key)
    # Irrecoverable errors. Discard the message to allow other messages to be processed.
    except FileNotFoundError:  # raised by detect_format() or open()
        logger.exception("%s has disappeared, skipping", collection_file.filename)
        create_note(collection, ERROR, f"{collection_file.filename} has disappeared", data=input_message)
        nack(client_state, channel, method.delivery_tag, requeue=False)
    except ijson.common.IncompleteJSONError:  # raised by ijson.parse()
        logger.exception("Source %s yields invalid JSON, skipping", collection.source_id)
        create_note(collection, ERROR, f"Source {collection.source_id} yields invalid JSON", data=input_message)
        nack(client_state, channel, method.delivery_tag, requeue=False)
    else:
        ack(client_state, channel, method.delivery_tag)


def process_file(collection_file) -> int | None:
    """
    Load file for a given collection.

    Create the collection_file and either the release, record or compiled_release.
    If the collection should be upgraded, create the same structure for upgraded collection as well.

    :param collection_file: collection file for which should be releases checked
    :returns: upgraded collection file id or None (if there is no upgrade planned)
    """
    data_type = collection_file.collection.data_type

    if data_type["format"] == Format.compiled_release:
        package = None
    else:
        package = _read_package_data_from_file(collection_file.filename, data_type)

    logger.debug("Writing data for collection_file %s", collection_file.pk)
    releases_or_records = _read_data_from_file(collection_file.filename, data_type)
    _store_data(collection_file, package, releases_or_records, data_type, upgrade=False)

    if upgraded_collection := collection_file.collection.get_upgraded_collection():
        upgraded_collection_file = CollectionFile(
            collection=upgraded_collection, filename=collection_file.filename, url=collection_file.url
        )
        upgraded_collection_file.save()

        logger.debug("Writing data for upgraded collection_file %s", upgraded_collection_file.pk)
        releases_or_records = _read_data_from_file(collection_file.filename, data_type)
        _store_data(upgraded_collection_file, package, releases_or_records, data_type, upgrade=True)

        return upgraded_collection_file.pk

    return None


def set_data_type(collection, collection_file):
    if not collection.data_type:
        detected_format, is_concatenated, is_array = detect_format(
            collection_file.filename, additional_prefixes=("extensions",)
        )

        # https://github.com/open-contracting/kingfisher-collect/issues/1012
        if detected_format == Format.empty_package:
            raise EmptyFormatError(f"Empty format '{detected_format}' for file {collection_file}.")
        if detected_format not in SUPPORTED_FORMATS:
            raise UnsupportedFormatError(
                f"Unsupported format '{detected_format}' for file {collection_file}. "
                f"Must be one of: {', '.join(sorted(SUPPORTED_FORMATS))}."
            )

        data_type = {
            "format": detected_format,
            "concatenated": is_concatenated,
            "array": is_array,
        }

        collection.data_type = data_type
        collection.save(update_fields=["data_type"])

        if upgraded_collection := collection.get_upgraded_collection():
            upgraded_collection.data_type = data_type
            upgraded_collection.save(update_fields=["data_type"])


class ControlCodesFilter:
    def __init__(self, file):
        self.file = file

    def read(self, buf_size):
        # Replace the "\u0000" escape sequence in the JSON string, which is rejected by PostgreSQL.
        # https://www.postgresql.org/docs/current/datatype-json.html
        return self.file.read(buf_size).replace(b"\\u0000", b"")


def _get_data_key(data_type):
    data_key = []

    if data_type["array"]:
        data_key.append("item")

    match data_type["format"]:
        case Format.record_package:
            data_key.extend(["records", "item"])
        case Format.release_package:
            data_key.extend(["releases", "item"])

    return ".".join(data_key)


def _read_package_data_from_file(filename, data_type):
    package = {}

    # If the file is an array of packages, only the first package metadata is extracted.
    package_key = "item" if data_type["array"] else ""
    data_key = _get_data_key(data_type).removesuffix(".item")

    with open(filename, "rb") as f:
        build = False
        builder = ObjectBuilder()

        # Constructs Decimal values. https://github.com/ICRAR/ijson#options
        for prefix, event, value in ijson.parse(ControlCodesFilter(f), multiple_values=True):
            if prefix == package_key:
                # Start of package.
                if event == "start_map":
                    build = True
                    builder = ObjectBuilder()
                # End of package.
                elif event == "end_map":
                    build = False
                    return builder.value

            if build and not prefix.startswith(data_key):
                builder.event(event, value)

    return package


def _read_data_from_file(filename, data_type):
    data_key = _get_data_key(data_type)

    with open(filename, "rb") as f:
        build = False
        builder = ObjectBuilder()

        # Constructs Decimal values. https://github.com/ICRAR/ijson#options
        for prefix, event, value in ijson.parse(ControlCodesFilter(f), multiple_values=True):
            if prefix == data_key:
                # Start of an item of data.
                if event == "start_map":
                    build = True
                    builder = ObjectBuilder()
                # End of an item of data.
                elif event == "end_map":
                    build = False
                    yield OrderedDict(builder.value)

            if build:
                builder.event(event, value)


def _store_data(collection_file, package, releases_or_records, data_type, *, upgrade=False):
    for release_or_record in releases_or_records:
        if upgrade:
            with create_logger_note(collection_file.collection, "ocdskit"):
                # upgrade_10_11() requires an OrderedDict. simplejson is used for native decimal support.
                # This requirement can be removed: https://github.com/open-contracting/ocdskit/issues/164
                release_or_record = upgrade_10_11(
                    json.loads(json.dumps(release_or_record, use_decimal=True), object_pairs_hook=OrderedDict)
                )

        data = get_or_create(Data, release_or_record)

        # The ocid is required to find all the releases relating to the same record, during compilation.
        if "ocid" not in release_or_record:
            logger.error("Skipped release or record without ocid: %s", release_or_record)
            continue

        match data_type["format"]:
            case Format.record_package:
                Record(
                    collection=collection_file.collection,
                    collection_file=collection_file,
                    package_data=get_or_create(PackageData, package),
                    data=data,
                    ocid=release_or_record["ocid"],
                ).save()
            case Format.release_package:
                Release(
                    collection=collection_file.collection,
                    collection_file=collection_file,
                    package_data=get_or_create(PackageData, package),
                    data=data,
                    ocid=release_or_record["ocid"],
                    release_id=release_or_record.get("id") or "",
                    release_date=release_or_record.get("date") or "",
                ).save()
            case Format.compiled_release:
                CompiledRelease(
                    collection=collection_file.collection,
                    collection_file=collection_file,
                    data=data,
                    ocid=release_or_record["ocid"],
                    release_date=release_or_record.get("date") or "",
                ).save()
