import logging

from django.db.models.functions import Now
from django.test import TransactionTestCase

from process.management.commands.finisher import completable
from process.models import Collection, CollectionFile, ProcessingStep

logging.getLogger("process.management.commands.finisher").setLevel(logging.INFO)


class CompletableTests(TransactionTestCase):
    fixtures = ["tests/fixtures/complete_db.json"]

    def test_already_completed(self):
        collection = Collection.objects.get(pk=3)
        collection.completed_at = Now()
        collection.save()

        self.assertEqual(completable(collection), False)

    def test_not_fully_processed(self):
        collection = Collection.objects.get(pk=2)
        collection_file = CollectionFile.objects.get(pk=1)
        collection_file_step = ProcessingStep(
            name=ProcessingStep.Name.LOAD,
            collection=Collection.objects.get(pk=2),
            collection_file=collection_file,
        )
        collection_file_step.save()

        self.assertEqual(completable(collection), False)

        collection_file_step.delete()

        collection.store_end_at = None
        collection.save()

        self.assertEqual(completable(collection), False)

    def test_happy_day_compiled(self):
        collection = Collection.objects.get(pk=2)
        collection.transform_type = Collection.Transform.COMPILE_RELEASES
        collection.store_end_at = None
        collection.compilation_started = True
        collection.save()

        self.assertEqual(completable(collection), True)
