from django.test import TransactionTestCase

from process.models import Collection
from process.processors.loader import create_collection_file


class CreateCollectionFileTests(TransactionTestCase):
    fixtures = ["tests/fixtures/complete_db.json"]

    def test_malformed_input(self):
        with self.assertRaises(ValueError) as e:
            create_collection_file(None, "wrong_path")
        self.assertEqual(str(e.exception), "collection None cannot be blank")

    def test_integrity_error(self):
        collection = Collection.objects.get(id=1)
        with self.assertRaises(ValueError) as e:
            create_collection_file(collection, "/path")
            create_collection_file(collection, "/path")
        self.assertTrue(str(e.exception).startswith("Collection 1 already contains file '/path'"))

    def test_happy_day(self):
        collection = Collection.objects.get(id=2)
        collection_file = create_collection_file(collection, "/path")

        self.assertEqual(collection_file.collection, collection)
