from django.db import transaction
from django.test import TransactionTestCase

from process.models import Collection, CollectionFile, CollectionFileStep
from process.processors.compiler import compilable


class CompilableTests(TransactionTestCase):
    fixtures = ["process/tests/fixtures/complete_db.json"]

    def test_malformed_input(self):
        with self.assertRaises(TypeError) as e:
            compilable("")
        self.assertEqual(str(e.exception), "collection_id is not an int value")

    def test_nonexistent_input(self):
        with self.assertRaises(ValueError) as e:
            compilable(5)
        self.assertEqual(str(e.exception), "Collection (with steps including compile) id 5 not found")

    def test_already_compiled(self):
        self.assertEqual(compilable(2), False)

    def test_not_fully_processed(self):
        collection = Collection.objects.get(id=2)
        collection_file = CollectionFile.objects.get(id=3)
        collection_file_step = CollectionFileStep()
        collection_file_step.name = "file_worker"
        collection_file_step.collection = collection
        collection_file_step.collection_file = collection_file
        collection_file_step.save()

        self.assertEqual(compilable(2), False)

        collection_file_step.delete()

        collection.store_end_at = None
        collection.save()
        self.assertEqual(compilable(2), False)

    def test_happy_day(self):
        with transaction.atomic():
            collection = Collection.objects.get(id=3)
            collection.delete()

        self.assertEqual(compilable(2), True)
