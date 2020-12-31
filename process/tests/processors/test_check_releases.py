from django.test import TransactionTestCase

from process.exceptions import AlreadyExists
from process.models import ReleaseCheck
from process.processors.checker import check_releases


class CheckReleasesTests(TransactionTestCase):
    fixtures = ["process/tests/fixtures/complete_db.json"]

    def test_malformed_input(self):
        with self.assertRaises(TypeError) as e:
            check_releases("")
        self.assertEqual(str(e.exception), "collection_file_id is not an int value")

    def test_nonexistent_input(self):
        with self.assertRaises(ValueError) as e:
            check_releases(155)
        self.assertEqual(str(e.exception), "Collection file id 155 not found")

    def test_already_compiled(self):
        with self.assertRaises(AlreadyExists):
            check_releases(2)

    def test_happy_day(self):
        count = ReleaseCheck.objects.filter(release__collection_file_item__collection_file=1).count()
        ReleaseCheck.objects.filter(release__collection_file_item__collection_file=1).delete()
        check_releases(1)

        self.assertEqual(count, 100)
