from django.db.models.functions import Now
from django.test import TransactionTestCase

from process.management.commands.finisher import completable
from process.models import Collection, CollectionFile, ProcessingStep


class CompletableTests(TransactionTestCase):
    fixtures = ["tests/fixtures/complete_db.json"]

    def test_already_completed(self):
        collection = Collection.objects.get(id=3)
        collection.completed_at = Now()
        collection.save()
        self.assertEqual(completable(3), False)

    def test_not_fully_processed(self):
        collection = Collection.objects.get(id=2)
        collection_file = CollectionFile.objects.get(id=1)
        collection_file_step = ProcessingStep()
        collection_file_step.name = ProcessingStep.Types.LOAD
        collection_file_step.collection = Collection.objects.get(id=2)
        collection_file_step.collection_file = collection_file
        collection_file_step.save()

        self.assertEqual(completable(2), False)

        collection_file_step.delete()

        collection.store_end_at = None
        collection.save()
        self.assertEqual(completable(2), False)

    def test_happy_day(self):
        self.assertEqual(completable(2), True)

    def test_happy_day_compiled(self):
        collection = Collection.objects.get(id=2)
        collection.transform_type = Collection.Transforms.COMPILE_RELEASES
        collection.store_end_at = None
        collection.compilation_started = True
        collection.save()
        self.assertEqual(completable(2), True)
