import logging

from django.db.utils import IntegrityError
from django.test import TransactionTestCase

from process.management.commands.checker import _check_collection_file
from process.models import CollectionFile, ReleaseCheck

logging.getLogger("process.management.commands.checker").setLevel(logging.INFO)


class CheckCollectionFileTests(TransactionTestCase):
    fixtures = ["tests/fixtures/complete_db.json"]

    def test_already_compiled(self):
        with self.assertRaises(IntegrityError):
            _check_collection_file(CollectionFile.objects.get(id=2))

    def test_happy_day(self):
        ReleaseCheck.objects.filter(release__collection_file_item__collection_file=1).delete()
        _check_collection_file(CollectionFile.objects.get(id=1))
        count = ReleaseCheck.objects.filter(release__collection_file_item__collection_file=1).count()

        self.assertEqual(count, 100)
