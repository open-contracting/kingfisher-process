from django.test import TransactionTestCase

from process.models import CollectionFile
from process.processors.file_loader import get_upgraded_collection


class GetUpgradedCollectionTests(TransactionTestCase):
    fixtures = ["tests/fixtures/complete_db.json"]

    def test_no_such_collection_input(self):
        collection_file = CollectionFile.objects.get(id=3)
        self.assertIsNone(get_upgraded_collection(collection_file))

    def test_happy_day(self):
        collection_file = CollectionFile.objects.get(id=1)

        upgraded_collection = get_upgraded_collection(collection_file)

        self.assertEqual(collection_file.collection, upgraded_collection.parent)
        self.assertEqual("upgrade-1-0-to-1-1", upgraded_collection.transform_type)
        self.assertTrue("upgrade" in collection_file.collection.steps)
