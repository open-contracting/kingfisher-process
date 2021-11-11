from django.test import TransactionTestCase

from process.exceptions import AlreadyExists
from process.models import CollectionFile, ReleaseCheck
from process.processors.checker import check_collection_file


class CheckCollectionFileTests(TransactionTestCase):
    fixtures = ["tests/fixtures/complete_db.json"]

    def test_malformed_input(self):
        with self.assertRaises(TypeError) as e:
            check_collection_file("")
        self.assertEqual(str(e.exception), "collection_file is not a CollectionFile value")

    def test_already_compiled(self):
        with self.assertRaises(AlreadyExists):
            check_collection_file(CollectionFile.objects.get(id=2))

    def test_happy_day(self):
        ReleaseCheck.objects.filter(release__collection_file_item__collection_file=1).delete()
        check_collection_file(CollectionFile.objects.get(id=1))
        count = ReleaseCheck.objects.filter(release__collection_file_item__collection_file=1).count()

        self.assertEqual(count, 100)
