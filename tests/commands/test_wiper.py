from django.test import TransactionTestCase, override_settings

from process.management.commands.wiper import delete_collection
from process.models import (
    Collection,
    CollectionFile,
    CompiledRelease,
    Data,
    PackageData,
    Release,
)
from tests.fixtures import collection


class WiperTests(TransactionTestCase):
    def build_collection(self):
        source = collection()
        source.save()

        collection_file = CollectionFile(collection=source, filename="x.json")
        collection_file.save()

        release_data = Data.objects.create(hash_md5="", data={"ocid": "ocds-1", "tag": ["tender"]})
        compiled_data = Data.objects.create(hash_md5="", data={"ocid": "ocds-1", "tag": ["compiled"]})
        package_data = PackageData.objects.create(hash_md5="", data={"version": "1.1"})

        Release.objects.create(
            collection=source,
            collection_file=collection_file,
            ocid="ocds-1",
            data=release_data,
            package_data=package_data,
        )
        CompiledRelease.objects.create(
            collection=source,
            collection_file=collection_file,
            ocid="ocds-1",
            data=compiled_data,
        )

        return source

    @override_settings(DEDUPLICATE_DATA=False)
    def test_deletes_data_if_no_deduplicate(self):
        source = self.build_collection()

        delete_collection(source.id)

        self.assertEqual(Collection.objects.count(), 0)
        self.assertEqual(Release.objects.count(), 0)
        self.assertEqual(CompiledRelease.objects.count(), 0)
        self.assertEqual(Data.objects.count(), 0)
        self.assertEqual(PackageData.objects.count(), 0)

    @override_settings(DEDUPLICATE_DATA=True)
    def test_keeps_data_if_deduplicate(self):
        source = self.build_collection()

        delete_collection(source.id)

        self.assertEqual(Collection.objects.count(), 0)
        self.assertEqual(Release.objects.count(), 0)
        self.assertEqual(CompiledRelease.objects.count(), 0)
        self.assertEqual(Data.objects.count(), 2)
        self.assertEqual(PackageData.objects.count(), 1)
