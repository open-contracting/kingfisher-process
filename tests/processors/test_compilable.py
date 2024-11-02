import logging

from django.test import TransactionTestCase

from process.management.commands.compiler import compilable
from process.models import Collection, CollectionFile, ProcessingStep

logging.getLogger("process.management.commands.compiler").setLevel(logging.INFO)


class CompilableTests(TransactionTestCase):
    fixtures = ["tests/fixtures/complete_db.json"]

    def test_already_compiled(self):
        collection = Collection.objects.get(pk=3)
        collection.compilation_started = True
        collection.save()
        self.assertEqual(compilable(collection), False)

    def test_not_fully_processed(self):
        collection = Collection.objects.get(pk=2)
        collection_file = CollectionFile.objects.get(pk=1)
        processing_step = ProcessingStep(
            name=ProcessingStep.Name.LOAD,
            collection=Collection.objects.get(pk=1),
            collection_file=collection_file,
        )
        processing_step.save()

        self.assertEqual(compilable(collection), False)

        processing_step.delete()

        collection.store_end_at = None
        collection.save()
        self.assertEqual(compilable(collection), False)
