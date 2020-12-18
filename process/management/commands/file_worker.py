import json
import sys
import ijson

from process.management.commands.base.worker import BaseWorker
from process.models import Collection, CollectionFile, CollectionFileItem, PackageData, Release, Data
from ocdskit.exceptions import UnknownFormatError
from ocdskit.upgrade import upgrade_10_11
from django.db import transaction
from process.util import get_hash
from ijson.common import ObjectBuilder


class Command(BaseWorker):

    worker_name = "file_worker"

    consume_keys = ["loader"]

    def __init__(self):
        super().__init__(self.worker_name)

    def process(self, channel, method, properties, body):
        try:
            # parse input message
            input_message = json.loads(body.decode('utf8'))
            self.debug("Received message {}".format(input_message))

            collection_file = CollectionFile.objects.prefetch_related('collection').get(
                pk=input_message["collection_file_id"])

            collection = collection_file.collection

            try:
                upgraded_collection = Collection.objects.filter(
                    transform_type__exact=Collection.Transforms.UPGRADE_10_11).get(parent=collection)
            except (Collection.DoesNotExist):
                upgraded_collection = None

            if upgraded_collection:
                upgraded_collection_file = collection_file
                upgraded_collection_file.pk = None
                upgraded_collection_file.collection = upgraded_collection
                upgraded_collection_file.save()

            try:
                with transaction.atomic():
                    with open(collection_file.filename, "rb") as f:
                        file_objects = []
                        package_data_object = None
                        builder_object = ObjectBuilder()
                        builder_package = ObjectBuilder()

                        build_object = False
                        build_package = False
                        for prefix, event, value in ijson.parse(f):
                            if prefix == "item" and event == 'start_map':
                                builder_package = ObjectBuilder()
                                build_package = True

                            if prefix == "item.releases.item" and event == 'start_map':
                                builder_object = ObjectBuilder()
                                build_object = True

                            if prefix == "item.releases.item" and event == 'end_map':
                                build_object = False
                                file_objects.append(builder_object.value)

                            if prefix == "item" and event == 'end_map':
                                build_package = False
                                package_data_object = builder_package.value

                            if build_object:
                                builder_object.event(event, value)

                            if build_package:
                                if not prefix.startswith("item.releases"):
                                    builder_package.event(event, value)

                        package_hash = get_hash(str(package_data_object))
                        try:
                            package_data = PackageData.objects.get(hash_md5=package_hash)
                        except (PackageData.DoesNotExist, PackageData.MultipleObjectsReturned):
                            package_data = PackageData()
                            package_data.data = package_data_object
                            package_data.hash_md5 = package_hash
                            package_data.save()

                        counter = 0
                        for item in file_objects:

                            collection_file_item = CollectionFileItem()
                            collection_file_item.collection_file = collection_file
                            collection_file_item.number = counter
                            counter += 1
                            collection_file_item.save()

                            item_hash = get_hash(str(item))

                            try:
                                data = Data.objects.get(hash_md5=item_hash)
                            except (Data.DoesNotExist, Data.MultipleObjectsReturned):
                                data = Data()
                                data.data = item
                                data.hash_md5 = item_hash
                                data.save()

                            release = Release()
                            release.collection = collection_file.collection
                            release.collection_file_item = collection_file_item
                            release.data = data
                            release.package_data = package_data
                            release.release_id = item["id"]
                            release.ocid = item["ocid"]
                            release.save()

                            if upgraded_collection:
                                item = upgrade_10_11(item)

                                collection_file_item = CollectionFileItem()
                                collection_file_item.collection_file = upgraded_collection_file
                                collection_file_item.number = counter
                                counter += 1
                                collection_file_item.save()

                                item_hash = get_hash(str(item))

                                try:
                                    data = Data.objects.get(hash_md5=item_hash)
                                except (Data.DoesNotExist, Data.MultipleObjectsReturned):
                                    data = Data()
                                    data.data = item
                                    data.hash_md5 = item_hash
                                    data.save()

                                release = Release()
                                release.collection = collection_file.collection
                                release.collection_file_item = collection_file_item
                                release.data = data
                                release.package_data = package_data
                                release.release_id = item["id"]
                                release.ocid = item["ocid"]
                                release.save()

                    self.deleteStep(collection_file)

                self.publish(json.dumps(input_message))
            except UnknownFormatError as e:
                self.exception("Uknown format for collection file id {}".format(collection_file.id))
                # save error
                collection_file.errors = {"exception": e}
                collection_file.save()

            # confirm message processing
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            self.exception(
                "Something went wrong when processing {}".format(body))
            sys.exit()
