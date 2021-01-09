import logging

import ijson
from django.db.utils import IntegrityError
from ijson.common import ObjectBuilder
from ocdskit.upgrade import upgrade_10_11

from process.exceptions import AlreadyExists
from process.models import Collection, CollectionFile, CollectionFileItem, Data, PackageData, Release
from process.util import get_hash

# Get an instance of a logger
logger = logging.getLogger("processor.file_loader")


def process_file(collection_file_id):
    """
    Loads file for a given collection - created the whole collection_file, collection_file_item etc. structure.
    If the collection should be upgraded, creates the same structure for upgraded collection as well.

    :param int collection_file_id: collection file id for which should be releases checked

    :returns: upgraded collection file id or None (if there is no upgrade planned)
    :rtype: int

    :raises TypeError: if there arent integers provided on input
    :raises ValueError: if there is no item of such id
    :raises ValueError: if there is no such "physical" file to load data from
    :raises AlreadyExists: if the collection file (or other items) already exists
    """

    # validate input
    if not isinstance(collection_file_id, int):
        raise TypeError("collection_file_id is not an int value")

    try:
        collection_file = CollectionFile.objects.get(id=collection_file_id)
        logger.info("Loading data for collection file {}".format(collection_file))

        collection_file = CollectionFile.objects.prefetch_related("collection").get(pk=collection_file_id)

        try:
            file_items, file_package_data = _read_data_from_file(collection_file.filename)
        except FileNotFoundError as e:
            raise ValueError("File '{}' not found".format(collection_file.filename)) from e

        _store_data(collection_file, file_items, file_package_data, False)

        upgraded_collection = get_upgraded_collection(collection_file)

        if upgraded_collection:
            upgraded_collection_file = _create_upgraded_collection_file(collection_file, upgraded_collection)
            _store_data(upgraded_collection_file, file_items, file_package_data, True)

            return upgraded_collection_file.id

        return None
    except CollectionFile.DoesNotExist:
        raise ValueError("Collection file id {} not found".format(collection_file_id))
    except IntegrityError as e:
        raise AlreadyExists("Item already exists".format(collection_file_id)) from e


def _read_data_from_file(filename):
    with open(filename, "rb") as f:
        file_items = []
        package_data_object = None
        builder_object = ObjectBuilder()
        builder_package = ObjectBuilder()

        build_object = False
        build_package = False
        for prefix, event, value in ijson.parse(f):
            if prefix == "item" and event == "start_map":
                builder_package = ObjectBuilder()
                build_package = True

            if prefix == "item.releases.item" and event == "start_map":
                builder_object = ObjectBuilder()
                build_object = True

            if prefix == "item.releases.item" and event == "end_map":
                build_object = False
                file_items.append(builder_object.value)

            if prefix == "item" and event == "end_map":
                build_package = False
                package_data_object = builder_package.value

            if build_object:
                builder_object.event(event, value)

            if build_package:
                if not prefix.startswith("item.releases"):
                    builder_package.event(event, value)

    return file_items, package_data_object


def _store_data(collection_file, file_items, file_package_data, upgrade=False):
    # store package data
    package_hash = get_hash(str(file_package_data))
    try:
        package_data = PackageData.objects.get(hash_md5=package_hash)
    except (PackageData.DoesNotExist, PackageData.MultipleObjectsReturned):
        package_data = PackageData()
        package_data.data = file_package_data
        package_data.hash_md5 = package_hash
        package_data.save()

    # store individual items
    counter = 0
    for item in file_items:
        # create and store item
        collection_file_item = CollectionFileItem()
        collection_file_item.collection_file = collection_file
        collection_file_item.number = counter
        counter += 1
        collection_file_item.save()

        # upgrade to latest version if necessary
        if upgrade:
            item = upgrade_10_11(item)

        # store data object
        item_hash = get_hash(str(item))
        try:
            data = Data.objects.get(hash_md5=item_hash)
        except (Data.DoesNotExist, Data.MultipleObjectsReturned):
            data = Data()
            data.data = item
            data.hash_md5 = item_hash
            data.save()

        # store release
        release = Release()
        release.collection = collection_file.collection
        release.collection_file_item = collection_file_item
        release.data = data
        release.package_data = package_data
        release.release_id = item["id"]
        release.ocid = item["ocid"]
        release.save()


def get_upgraded_collection(collection_file):
    """
    Gets upgraded collection for collection_file.parent collection. Throws an exception if such
    collection does not exists.

    :param CollectionFile collection_file: collection file of the parent

    :returns: upgraded collection
    :rtype: Collection

    :raises TypeError: if there arent integers provided on input
    :raises ValueError: if there is no such collection
    """
    # validate input
    if not isinstance(collection_file, CollectionFile):
        raise TypeError("collection_file is not an instance of CollectionFile")

    try:
        upgraded_collection = Collection.objects.filter(transform_type__exact=Collection.Transforms.UPGRADE_10_11).get(
            parent=collection_file.collection
        )
    except Collection.DoesNotExist:
        raise ValueError(
            "There is no upgrade collection for collection {} (via collection_file {})".format(
                collection_file.collection, collection_file
            )
        )
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
