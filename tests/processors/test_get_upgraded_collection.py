from django.test import TransactionTestCase

from process.management.commands.file_worker import _get_upgraded_collection
from process.models import CollectionFile


class GetUpgradedCollectionTests(TransactionTestCase):
    fixtures = ["tests/fixtures/complete_db.json"]

    def test_no_such_collection_input(self):
        collection_file = CollectionFile.objects.get(id=3)
        self.assertIsNone(_get_upgraded_collection(collection_file))

    def test_happy_day(self):
        collection_file = CollectionFile.objects.get(id=1)

        upgraded_collection = _get_upgraded_collection(collection_file)

        self.assertEqual(collection_file.collection, upgraded_collection.parent)
        self.assertEqual("upgrade-1-0-to-1-1", upgraded_collection.transform_type)
        self.assertTrue("upgrade" in collection_file.collection.steps)
