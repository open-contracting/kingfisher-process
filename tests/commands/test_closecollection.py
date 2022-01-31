from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TransactionTestCase

from tests.fixtures import collection


class CloseTests(TransactionTestCase):
    def test_missing_args(self):
        with self.assertRaises(CommandError) as e:
            call_command("closecollection")

        message = "Error: the following arguments are required: collection_id"
        self.assertEqual(str(e.exception), message)

    def test_wrong_args(self):
        with self.assertRaises(ValueError) as e:
            call_command("closecollection", "text")

        message = "Field 'id' expected a number but got 'text'."
        self.assertEqual(str(e.exception), message)

    def test_ok(self):
        source = collection()
        source.save()
        call_command("closecollection", source.id)
        source.refresh_from_db()

        self.assertIsNotNone(source.store_end_at)
