from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TransactionTestCase

from process.tests.fixtures import collection


class CloseTests(TransactionTestCase):
    def test_missing_args(self):
        with self.assertRaises(CommandError) as e:
            call_command("close")

        message = "Please indicate collection by its id."
        self.assertEqual(str(e.exception), message)

    def test_wrong_args(self):
        with self.assertRaises(CommandError) as e:
            call_command("close", "-c", "dfds")

        message = "--collection dfds is not an int value"
        self.assertEqual(str(e.exception), message)

    def test_ok(self):
        source = collection()
        source.save()
        call_command("close", "-c", source.id)
        source.refresh_from_db()

        self.assertIsNotNone(source.store_end_at)
