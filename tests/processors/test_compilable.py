import logging

from django.test import TransactionTestCase

from process.management.commands.compiler import compilable
from process.models import Collection, CollectionFile, ProcessingStep

logging.getLogger("process.management.commands.compiler").setLevel(logging.INFO)


class CompilableTests(TransactionTestCase):
    fixtures = ["tests/fixtures/complete_db.json"]

    def test_already_compiled(self):
        collection = Collection.objects.get(id=3)
        collection.compilation_started = True
        collection.save()
        self.assertEqual(compilable(collection), False)

    def test_not_fully_processed(self):
        collection = Collection.objects.get(id=2)
        collection_file = CollectionFile.objects.get(id=1)
        collection_file_step = ProcessingStep(
            name=ProcessingStep.Name.LOAD,
            collection=Collection.objects.get(id=1),
            collection_file=collection_file,
        )
        collection_file_step.save()

        self.assertEqual(compilable(collection), False)

        collection_file_step.delete()

        collection.store_end_at = None
        collection.save()
        self.assertEqual(compilable(collection), False)
