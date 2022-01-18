from django.test import TransactionTestCase

from process.models import Collection, CollectionFile, ProcessingStep
from process.processors.compiler import compilable


class CompilableTests(TransactionTestCase):
    fixtures = ["tests/fixtures/complete_db.json"]

    def test_nonexistent_input(self):
        self.assertEqual(compilable(5), False)

    def test_already_compiled(self):
        collection = Collection.objects.get(id=3)
        collection.compilation_started = True
        collection.save()
        self.assertEqual(compilable(2), False)

    def test_not_fully_processed(self):
        collection = Collection.objects.get(id=2)
        collection_file = CollectionFile.objects.get(id=1)
        collection_file_step = ProcessingStep()
        collection_file_step.name = ProcessingStep.Types.LOAD
        collection_file_step.collection = Collection.objects.get(id=1)
        collection_file_step.collection_file = collection_file
        collection_file_step.save()

        self.assertEqual(compilable(2), False)

        collection_file_step.delete()

        collection.store_end_at = None
        collection.save()
        self.assertEqual(compilable(2), False)

    def test_happy_day(self):
        self.assertEqual(compilable(2), True)
