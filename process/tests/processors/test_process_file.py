from django.test import TransactionTestCase

from process.models import CollectionFile, CollectionFileItem, PackageData, Release
from process.processors.file_loader import process_file


class ProcessFileTests(TransactionTestCase):
    fixtures = ["process/tests/fixtures/complete_db.json"]

    def test_malformed_input(self):
        with self.assertRaises(TypeError) as e:
            process_file("")
        self.assertEqual(str(e.exception), "collection_file_id is not an int value")

        with self.assertRaises(ValueError) as e:
            process_file(10000000)
        self.assertEqual(str(e.exception), "Collection file id 10000000 not found")

    def test_file_not_found(self):
        with self.assertRaises(ValueError) as e:
            process_file(5)
        self.assertEqual(str(e.exception), "File for collection file id:5 not found")

    def test_happy_day(self):
        collection_file = CollectionFile.objects.get(id=1)
        collection_file.filename = "process/tests/fixtures/collection_file.json"
        collection_file.save()

        CollectionFileItem.objects.filter(collection_file=collection_file).delete()
        CollectionFile.objects.get(id=3).delete()

        upgraded_collection_file_id = process_file(1)

        upgraded_collection_file = CollectionFile.objects.get(id=upgraded_collection_file_id)

        self.assertEqual(upgraded_collection_file.filename, collection_file.filename)
        self.assertEqual(upgraded_collection_file.filename, "process/tests/fixtures/collection_file.json")
        self.assertEqual(upgraded_collection_file.collection.parent.id, collection_file.collection.id)

        self.assertEqual(CollectionFileItem.objects.filter(collection_file=collection_file).count(), 1)
        self.assertEqual(CollectionFileItem.objects.filter(collection_file=upgraded_collection_file).count(), 1)

        self.assertEqual(
            PackageData.objects.filter(release__collection_file_item__collection_file=collection_file)
            .distinct()
            .count(),
            1,
        )
        self.assertEqual(
            PackageData.objects.filter(release__collection_file_item__collection_file=upgraded_collection_file)
            .distinct()
            .count(),
            1,
        )

        self.assertEqual(Release.objects.filter(collection_file_item__collection_file=collection_file).count(), 100)
        self.assertEqual(
            Release.objects.filter(collection_file_item__collection_file=upgraded_collection_file).count(), 100
        )

        self.assertEqual(upgraded_collection_file.filename, "process/tests/fixtures/collection_file.json")
        self.assertEqual(upgraded_collection_file.filename, "process/tests/fixtures/collection_file.json")
        self.assertEqual(upgraded_collection_file.filename, "process/tests/fixtures/collection_file.json")
        self.assertEqual(upgraded_collection_file.filename, "process/tests/fixtures/collection_file.json")
