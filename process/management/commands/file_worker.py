import logging
from collections import OrderedDict

import ijson
import simplejson as json
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.utils import IntegrityError
from ijson.common import ObjectBuilder
from ocdskit.upgrade import upgrade_10_11
from ocdskit.util import detect_format
from yapw.methods.blocking import ack, nack, publish

from process.models import (
    Collection,
    CollectionFile,
    CollectionFileItem,
    Data,
    PackageData,
    ProcessingStep,
    Record,
    Release,
)
from process.util import consume, create_step, decorator, delete_step, get_hash

consume_routing_keys = ["loader", "api_loader"]
routing_key = "file_worker"
logger = logging.getLogger(__name__)

SUPPORTED_FORMATS = [Collection.DataTypes.RELEASE_PACKAGE, Collection.DataTypes.RECORD_PACKAGE]


class Command(BaseCommand):
    def handle(self, *args, **options):
        consume(callback, routing_key, consume_routing_keys, decorator=decorator, prefetch_count=20)


def callback(client_state, channel, method, properties, input_message):
    collection_id = input_message["collection_id"]
    collection_file_id = input_message["collection_file_id"]

    try:
        with delete_step(ProcessingStep.Types.LOAD, collection_file_id=collection_file_id):
            with transaction.atomic():
                collection_file = CollectionFile.objects.select_related("collection").get(pk=collection_file_id)
                upgraded_collection_file_id = process_file(collection_file)

        # If a duplicate message is received causing an IntegrityError above, we still want to create the next step, in
        # case it was not created the first time. delete_step() will delete any duplicate steps.
        if settings.ENABLE_CHECKER:
            create_step(ProcessingStep.Types.CHECK, collection_id, collection_file_id=collection_file_id)

        message = {"collection_id": collection_id, "collection_file_id": collection_file_id}
        publish(client_state, channel, message, routing_key)

        if upgraded_collection_file_id:
            if settings.ENABLE_CHECKER:
                create_step(ProcessingStep.Types.CHECK, collection_id, collection_file_id=upgraded_collection_file_id)

            message = {"collection_id": collection_id, "collection_file_id": upgraded_collection_file_id}
            publish(client_state, channel, message, routing_key)
    # An irrecoverable error, raised by ijson.parse(). Discard the message to allow other messages to be processed.
    except ijson.common.IncompleteJSONError:
        logger.exception("Spider %s yields invalid JSON", collection_file.collection.source_id)
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

    logger.info("Loading data for collection file %s", collection_file)

    # detect format and check, whether its supported
    data_type = _get_data_type(collection_file)

    if data_type["format"] not in SUPPORTED_FORMATS:
        raise ValueError(
            "Unsupported data type '{}' for file {}. Must be one of {}".format(
                data_type, collection_file, SUPPORTED_FORMATS
            )
        )

    # read the file data
    file_items, file_package_data = _read_data_from_file(collection_file.filename, data_type)

    # store data for a current collection
    _store_data(collection_file, file_items, file_package_data, data_type, False)

    # should we store upgraded data as well?
    upgraded_collection = _get_upgraded_collection(collection_file)

    if upgraded_collection:
        upgraded_collection_file = _create_upgraded_collection_file(collection_file, upgraded_collection)
        _store_data(upgraded_collection_file, file_items, file_package_data, data_type, True)

        # return upgraded file
        return upgraded_collection_file.pk

    # not upgrading, return None
    return None


def _read_data_from_file(filename, data_type):
    key = ""
    package_key = ""
    # is there an aray with data?
    if data_type["array"]:
        key = "item."
        package_key = "item"

    # build key based on what we are handling
    if data_type["format"] == Collection.DataTypes.RECORD_PACKAGE:
        key = key + "records"
    elif data_type["format"] == Collection.DataTypes.RELEASE_PACKAGE:
        key = key + "releases"
    else:
        raise ValueError(
            "Unsupported format {} for {}, must be one of {}".format(data_type, filename, SUPPORTED_FORMATS)
        )

    with open(filename, "rb") as f:
        file_items = []
        package_data_object = None
        builder_object = ObjectBuilder()
        builder_package = ObjectBuilder()

        build_object = False
        build_package = False
        for prefix, event, value in ijson.parse(f, use_float=True):

            if prefix == package_key and event == "start_map":
                # collection of package data started
                builder_package = ObjectBuilder()
                build_package = True

            if prefix == "{}.item".format(key) and event == "start_map":
                # collection of the record/release data item started here
                builder_object = ObjectBuilder()
                build_object = True

            if prefix == "{}.item".format(key) and event == "end_map":
                # collection of the record/release data item ended here
                build_object = False
                file_items.append(OrderedDict(builder_object.value))

            if prefix == package_key and event == "end_map":
                # collection of package data ended
                build_package = False
                package_data_object = builder_package.value

            if build_object:
                # store data to current record/release item
                builder_object.event(event, value)

            if build_package:
                # store data to package object
                if not prefix.startswith(key):
                    builder_package.event(event, value)

    return file_items, package_data_object


def _store_data(collection_file, file_items, file_package_data, data_type, upgrade=False):
    # store package data
    package_hash = get_hash(str(file_package_data))
    try:
        package_data = PackageData.objects.get(hash_md5=package_hash)
    except (PackageData.DoesNotExist, PackageData.MultipleObjectsReturned):
        package_data = PackageData()
        package_data.data = file_package_data
        package_data.hash_md5 = package_hash
        try:
            with transaction.atomic():
                # another transaction needed here as integrity error will "broke" the upper one
                package_data.save()
        except IntegrityError:
            package_data = PackageData.objects.get(hash_md5=package_hash)

    # store individual items
    collection_file_item = CollectionFileItem()
    collection_file_item.collection_file = collection_file
    collection_file_item.number = 0
    collection_file_item.save()

    for item in file_items:
        # upgrade to latest version if necessary
        if upgrade:
            # this is not the prettiest solution
            # however there is no way to tell upgrade_10_11 to not to reorder keys
            # simplejson is used here as it supports Decimal natively
            item = upgrade_10_11(json.loads(json.dumps(item, use_decimal=True), object_pairs_hook=OrderedDict))

        # store data object
        item_hash = get_hash(str(item))
        try:
            data = Data.objects.get(hash_md5=item_hash)
        except (Data.DoesNotExist, Data.MultipleObjectsReturned):
            data = Data()
            data.data = item
            data.hash_md5 = item_hash
            try:
                with transaction.atomic():
                    # another transaction needed here as integrity error will "broke" the upper one
                    data.save()
            except IntegrityError:
                data = Data.objects.get(hash_md5=item_hash)

        if data_type["format"] == Collection.DataTypes.RECORD_PACKAGE:
            # store record
            record = Record()
            record.collection = collection_file.collection
            record.collection_file_item = collection_file_item
            record.data = data
            record.package_data = package_data
            record.ocid = item["ocid"]
            record.save()
        elif data_type["format"] == Collection.DataTypes.RELEASE_PACKAGE:
            # store release
            release = Release()
            release.collection = collection_file.collection
            release.collection_file_item = collection_file_item
            release.data = data
            release.package_data = package_data
            release.release_id = item["id"]
            release.ocid = item["ocid"]
            release.save()
        else:
            raise ValueError(
                "Unsupported format {} for {}, must be one of {}".format(format, collection_file, SUPPORTED_FORMATS)
            )


def _get_data_type(collection_file):
    """
    Returns the expected data type of the collection_file.
    """
    collection = collection_file.collection
    if not collection.data_type:
        detected_format = detect_format(collection_file.filename)
        collection.set_data_type(detected_format)
        collection.save(update_fields=["data_type"])
        upgraded_collection = collection.get_upgraded_collection()
        if upgraded_collection:
            upgraded_collection.set_data_type(detected_format)
            upgraded_collection.save(update_fields=["data_type"])

    return collection.data_type


def _get_upgraded_collection(collection_file):
    """
    Gets upgraded collection for collection_file.parent collection.

    :param CollectionFile collection_file: collection file of the parent

    :returns: upgraded collection
    :rtype: Collection
    """
    try:
        upgraded_collection = Collection.objects.filter(transform_type=Collection.Transforms.UPGRADE_10_11).get(
            parent_id=collection_file.collection_id
        )
    except Collection.DoesNotExist:
        return None
    return upgraded_collection


def _create_upgraded_collection_file(collection_file, upgraded_collection):
    """
    Simple helper responsible for "copying" existing collection file to an upgraded_collection.collection_file.
    """
    upgraded_collection_file = CollectionFile()
    upgraded_collection_file.collection = upgraded_collection
    upgraded_collection_file.filename = collection_file.filename
    upgraded_collection_file.url = collection_file.url
    upgraded_collection_file.save()

    return upgraded_collection_file
